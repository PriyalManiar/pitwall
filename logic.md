# PitWall — Engineering Decision Log

> This document captures every non-obvious design decision made during the
> build of PitWall, along with the problem it solved, the alternatives
> considered, and the rationale for the final choice.
> 
> The goal is not to justify every line of code — it is to demonstrate that
> every architectural choice was deliberate, not accidental.

---

## 001 — Weather + Lap Time Join Strategy

### Problem
Weather data is sampled every ~15 seconds throughout a session. Lap times
occur roughly every 90 seconds. The two datasets have different frequencies
and cannot be joined on an exact timestamp match — there will never be a
weather reading at the exact moment a lap starts or ends.

### Options Considered

**Option A — Aggregate weather to 90 second buckets, then join**
Group weather readings into 90 second windows using mean for continuous
variables (TrackTemp, AirTemp, Humidity) and max for Rainfall. Join to laps
on the bucket timestamp.

Problem: assumes all laps are exactly 90 seconds and perfectly aligned to
bucket boundaries. In reality lap lengths vary significantly — lap 1 is
~105 seconds, safety car laps can exceed 120 seconds, pit laps are longer.
Bucket misalignment can assign weather from 45+ seconds before or after the
lap actually occurred.

**Option B — AsOf join (chosen)**
For each lap, find the closest actual weather reading by timestamp regardless
of lap length. No bucketing, no alignment assumption. Handles variable lap
lengths automatically.

### Decision
AsOf join using `pd.merge_asof` with `direction='nearest'`.

### Rationale
Lap length variability makes fixed bucket aggregation unreliable. An asof
join is more accurate because it matches each lap to the weather reading
closest in time to when that lap actually occurred — regardless of whether
it was a 88 second flying lap or a 130 second safety car lap.

For continuous variables like TrackTemp the difference is small (~0.1°C
error with bucketing). For Rainfall the difference is critical — rain can
start and stop within minutes, and a bucket misalignment can assign the
wrong rainfall status to a lap, which would corrupt both the weather impact
analysis and the ML pit stop prediction model.

### Implementation Note
```python
laps_with_weather = pd.merge_asof(
    session.laps.sort_values('Time'),
    session.weather_data.sort_values('Time'),
    on='Time',
    direction='nearest'
)
```

### dbt Implication
The asof join logic is handled in the ingestion layer (Python) before data
lands in Snowflake. The `stg_lap_times` model receives weather columns
already joined — it does not need to re-implement the join in SQL.

---

## 002 — Pit Stop Reconstruction Logic

### Problem
A pit stop physically spans two laps but FastF1 stores the timing data
across two separate rows — the entry time on one lap and the exit time on
the next. There is no single row representing a complete pit stop event.

Specifically:
- `PitInTime` (when the car crossed the pit entry line) lives on the **last
  lap of a stint**
- `PitOutTime` (when the car crossed the pit exit line) lives on the **first
  lap of the next stint**
- On any given lap, only one of these fields is populated — the other is
  always `NaT`
- Filtering for both `PitInTime.notna() AND PitOutTime.notna()` returns zero
  rows by design

### Options Considered

**Option A — Filter on a single column only**
Filter `PitInTime.notna()` to get pit laps, ignore `PitOutTime`.
Gives you the lap a driver pitted but not the duration or new compound.

**Option B — Two separate filters joined on consecutive stints (chosen)**
Filter `PitInTime.notna()` into one variable (the IN lap).
Filter `PitOutTime.notna()` into a separate variable (the OUT lap).
Join the two on consecutive stint numbers to reconstruct the complete event.

### Decision
Two separate filtered DataFrames joined on `Stint` and `Stint + 1`.

### Rationale
Pit stop duration (`PitOutTime - PitInTime`) and new compound are both
required for strategy analysis. Duration alone tells you operational
efficiency. New compound tells you the strategy call. Neither is available
from a single-column filter — the complete event only exists after joining
the two sides.

This is not a workaround — it is the correct representation of the physical
event. The car enters the pits on one lap and exits on the next. The data
model reflects reality.

### Implementation Note
```python
# The IN side — last lap of the stint
pit_in = laps[laps['PitInTime'].notna()][
    ['Driver', 'LapNumber', 'Stint', 'PitInTime', 'Compound']
]

# The OUT side — first lap of the new stint
pit_out = laps[laps['PitOutTime'].notna()][
    ['Driver', 'LapNumber', 'Stint', 'PitOutTime', 'Compound']
]

# Join on consecutive stints to reconstruct the full event
pit_stops = pit_in.merge(
    pit_out,
    left_on=['Driver', 'Stint'],
    right_on=['Driver', 'Stint'],
    suffixes=('_in', '_out')
)

pit_stops['pit_duration_seconds'] = (
    pit_stops['PitOutTime'] - pit_stops['PitInTime']
)
```

### dbt Implication
The `stg_pit_stops` model is derived from `stg_lap_times` using this same
logic in SQL — a self join on consecutive stint numbers per driver per race.
It is not a separate raw source. One raw table feeds two staging models.

---

## 003 — Telemetry Storage Strategy

### Problem
Raw telemetry is sampled at ~3.7Hz — approximately one reading every 5
metres of track. A single race produces ~300,000 rows of telemetry across
all drivers. The full 2024 season produces ~7 million rows. Storing all of
this at full resolution in Snowflake is expensive and unnecessary for most
analytical questions.

However, aggregating telemetry to one row per lap before storage destroys
the circuit-specific corner speed signals that drive the ML pit stop
prediction model. Tire degradation is caused by specific high-load corners —
not by average speed across the whole lap. Aggregation would compromise
model accuracy.

### Options Considered

**Option A — Aggregate everything to one row per lap**
Store max speed, avg speed, full throttle %, heavy braking % per lap.
Simple, cheap, fast to query.

Problem: loses corner-specific signals. Cannot reconstruct which corners
caused tire stress. ML model loses its strongest predictive features.
Gap-to-car-ahead calculations become impossible without positional data.

**Option B — Store full raw telemetry for all columns**
Store all 15 telemetry columns at full resolution for all 24 races.
Maximum accuracy, maximum flexibility.

Problem: 7 million rows of wide data in Snowflake. High storage cost.
Slow ingestion. Most of the data (RPM, Gear, Z coordinate, Status) is
never queried by dashboards or the model.

**Option C — Two separate tables with different purposes (chosen)**
Store raw telemetry with only the columns needed for ML feature engineering.
Store aggregated telemetry per lap for dashboard queries.

### Decision
Two Snowflake tables from one ingestion script:

**`raw.telemetry_raw`**
Columns: Driver, LapNumber, Distance, Speed, Brake, Throttle, X, Y,
Race, Year
Filter: Source = 'car' only — interpolated points excluded
Purpose: ML pit stop prediction model feature engineering
Rationale: Feature extraction happens at model training time, not ingestion
time. Corner-specific metrics (minimum speed at high-load corners, braking
event intensity) are derived from raw distance/speed data during training.
Aggregating before storage would permanently destroy these signals.

**`mart.telemetry_lap_agg`**
Columns: Driver, LapNumber, MaxSpeed, MinSpeed, AvgSpeed, FullThrottlePct,
HeavyBrakingPct, DrsOpenPct, Race, Year
One row per lap per driver — ~1,500 rows for the full season
Purpose: Looker dashboards — driver style analysis, pace comparison
Rationale: Dashboards do not need per-metre resolution. Aggregation reduces
7M rows to 1,500 without losing any information relevant to visualisation.

### Rationale
The key insight is that **storage granularity and modelling granularity are
different concerns**. Dashboards need lap-level aggregates. The ML model
needs raw distance-speed pairs to extract corner-specific features. Storing
two purpose-built tables serves both use cases without compromise.

This also separates the storage concern (ingest what you need) from the
modelling concern (extract features at training time) — a standard pattern
in production ML pipelines.

### Implementation Note
```python
# Raw store — filtered columns, car data only
tel_raw = tel[tel['Source'] == 'car'][[
    'Driver', 'LapNumber', 'Distance',
    'Speed', 'Brake', 'Throttle',
    'X', 'Y', 'Race', 'Year'
]]

# Aggregated store — one row per lap for dashboards
tel_agg = tel.groupby(['Driver', 'LapNumber']).agg(
    max_speed=('Speed', 'max'),
    min_speed=('Speed', 'min'),
    avg_speed=('Speed', 'mean'),
    full_throttle_pct=('Throttle', lambda x: (x == 100).mean()),
    heavy_braking_pct=('Brake', 'mean'),
    drs_open_pct=('DRS', lambda x: (x >= 10).mean())
).reset_index()
```

---

## 004 — Qualifying vs Race Data Usage

### Problem
Qualifying and race sessions produce fundamentally different data. Using
the wrong session for the wrong analytical question produces misleading
results.

### Decision
**Race session (`'R'`)** for all strategy, championship, and tire analysis.
**Qualifying session (`'Q'`, Q3 times only)** for pace baselines and
constructor upgrade detection.
**Practice sessions (`'FP1-3'`)** excluded entirely.
**Sprint sessions (`'S'`)** excluded entirely.

### Rationale

**Why Q3 only for qualifying pace:**
Q1 and Q2 involve drivers on different tire strategies — midfield teams
often save their best compound for the race during Q2. Q3 puts the top 10
on a level playing field with maximum attack, fresh tires, no fuel strategy
consideration. It is the cleanest single-lap pace signal available.

**Why qualifying is better than race for upgrade detection:**
Race pace is noisy — traffic, safety cars, strategy calls, and tire
management all obscure the underlying car performance. A qualifying lap
delta between two consecutive rounds is the cleanest signal that an
upgrade worked. Telemetry corner speeds in qualifying catch upgrade effects
one race earlier than race pace data.

**Why sprints are excluded:**
Only 6 of 24 rounds have sprint races. Sprint format (17-19 laps, usually
no pit stops, single compound) is fundamentally different from a full race.
Including sprint data in tire degradation models or pace trend analysis
would mix two incompatible formats and reduce the statistical validity of
any insight derived.

**Why practice is excluded:**
Practice sessions use experimental setups, fuel loads vary, and drivers
run programmes designed to gather data rather than lap quickly. Practice
times are not representative of race or qualifying pace.

---

## 005 — Pre-2018 vs Post-2018 Data Regime

### Problem
FastF1 session data (lap times, telemetry, tire data) is only reliably
available from 2018 onwards. The Ergast API wrapper within FastF1 provides
race results and standings back to 1950 — but with no telemetry, no tire
data, and no lap-level granularity.

This creates two data regimes within the same tool:
- **Pre-2018:** results only — positions, points, DNF status
- **Post-2018:** full session data — lap times, tire strategy, telemetry

### Decision
Build a unified driver performance mart that handles both regimes
gracefully. The mart exposes a consistent schema to Looker regardless of
the era being queried. Columns unavailable in the pre-2018 regime are
`NULL` with a `data_regime` flag indicating which era the row belongs to.

### Rationale
The historical champions analysis requires pre-2018 data for KPIs like
win rate, championship margin, and dominance score across all eras.
Restricting to post-2018 would make the analysis incomplete and limit
comparisons to a 6-year window.

The `data_regime` flag is a deliberate design choice — it makes the
limitation transparent rather than hiding it. Looker filters and
annotations can use it to explain to dashboard viewers why some columns
are empty for pre-2018 rows.

### dbt Implication
The `int_driver_era_unified` intermediate model handles the regime join.
The `mart_historical_champions` model surfaces the unified schema with
the `data_regime` column exposed as a Looker dimension.

---

*Last updated: March 2026*
*Author: Priyal Maniar*
*Project: PitWall — F1 Race Intelligence Pipeline*