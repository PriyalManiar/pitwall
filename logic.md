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

## 006 — Position Fields and DNF Handling

### Problem
The results dataset contains three position-related fields that are 
easily confused, and drivers do not complete equal laps due to retirements.
Using the wrong position field or including DNF laps in pace calculations
produces misleading analytics.

### Position Fields — What Each Means

**`GridPosition`** — where the driver started the race. Determined by
qualifying. Used for: positions gained/lost metric.

**`Position`** — where the driver finished on the road. Physical crossing
order at the chequered flag. Can differ from official result due to
post-race penalties. Used for: race narrative, lap-by-lap position tracking.

**`ClassifiedPosition`** — the official FIA result after stewards apply
penalties, time additions, and classification rules. Drivers who retire
but complete 90%+ of race distance are still classified. This is what
feeds championship points. Used for: all championship calculations.

### Why Lap Counts Are Unequal Across Drivers

A naive assumption is that all drivers complete the same number of laps.
In reality:
- DNF drivers (Accident, Engine, Gearbox etc) stop at their retirement lap
- Lap 1 incidents can result in 0 or 1 lap rows
- Classified-but-retired drivers have fewer laps than the race winner
- Lapped drivers complete the same lap count as the leader — the race
  ends when the leader crosses the line, everyone stops simultaneously

### Decision

**For championship points:** always use `ClassifiedPosition` — it is the
official result and the only field that correctly maps to points awarded.

**For race narrative and position tracking:** use `Position` — it reflects
what physically happened on track including battles and overtakes.

**For positions gained metric:** `GridPosition - ClassifiedPosition` — the
delta between where a driver started and where they officially finished.
This is a key column in `mart_driver_season_performance` for separating
driver skill from car advantage.

**For pace calculations:** exclude DNF laps entirely. A driver who retired
on lap 5 with a slow out-lap has a misleading average lap time. Filter
condition in all pace models:
```sql
where status = 'Finished'
or classified_position is not null
```

**For team pace averages:** weight by laps completed. A teammate who only
completed 10 laps should not equally influence a team's average pace
calculation alongside a driver who completed 57 laps.

### Rationale
Championship analysis built on `Position` instead of `ClassifiedPosition`
produces incorrect points totals — particularly in seasons with multiple
post-race penalties (2021, 2023). The positions gained metric using
`GridPosition` is one of the cleanest signals of driver performance
independent of car quality — a driver consistently gaining positions from
mid-grid is demonstrating racecraft regardless of machinery.

DNF lap filtering is a data quality decision, not a business logic decision.
Unrepresentative laps — retirement laps, lap 1 chaos laps, safety car
laps — pollute pace models and degrade ML feature quality. Filtering them
is correct engineering, not cherry-picking.

### dbt Implication
- `stg_results` exposes all three position fields with clear column
  descriptions in schema.yml
- `mart_driver_season_performance` uses `ClassifiedPosition` for points,
  `Position` for race narrative columns, and derives `positions_gained`
  as `grid_position - classified_position`
- All intermediate pace models include a `is_representative_lap` boolean
  flag — True when the lap is accurate, not a DNF lap, not under safety
  car, and not lap 1. Downstream models filter on this flag rather than
  re-implementing the logic independently

  ## 007 — Safety Car and VSC Lap Exclusion + Pit Stop Strategy Impact

### Problem
Formula 1 races are frequently interrupted by Safety Car (SC) and Virtual
Safety Car (VSC) periods. Laps completed under these conditions are
fundamentally different from racing laps and corrupt three analytical
models if included.

TrackStatus values in FastF1:
- '1' = green flag, track clear
- '2' = yellow flag in a sector  
- '4' = full safety car deployed
- '5' = virtual safety car (VSC)

### What SC and VSC Do to Lap Data

**Safety Car (4):** A physical safety car leads the field at 80-120 km/h.
All drivers queue behind it, no overtaking permitted. Lap times inflate
to roughly 2:10 vs a normal 1:36. The lap time reflects road car pace,
not race car pace.

**VSC (5):** No physical car. Drivers must hit a minimum delta time on
their steering wheel display. Less dramatic than SC but still artificially
slow and not representative of race pace.

### Why SC/VSC Laps Must Be Excluded

**Tire degradation models:** A SC lap puts near-zero stress on tires.
If included, degradation curves show a false "recovery" mid-stint — the
tires appear to improve in condition which is physically impossible. The
recovery is just a rest lap, not real tire regeneration.

**Pace analysis:** Average lap time drops significantly if SC laps are
included, making drivers appear slower than their actual race pace. A
driver with two SC laps in a stint looks 3-4 seconds per lap slower on
average than one with a clean stint.

**Pit stop prediction ML model:** SC laps are the single biggest noise
source in the feature table. Teams almost always pit under SC because
it costs minimal time. If the model sees SC laps in training data it
learns the wrong relationship between tire age and pit decisions — it
conflates reactive SC pitting with proactive strategic pitting.

### Decision
Exclude all laps where TrackStatus contains '4' or '5' from pace
analysis, tire degradation models, and ML feature tables via the
is_rep_lap flag.
```python
~laps['TrackStatus'].astype(str).str.contains('4|5')
```

`.astype(str)` applied first because FastF1 returns TrackStatus
inconsistently as string or numeric depending on the session. This
normalises before the contains check.

### Pit Stop Strategy — SC Pitting as a Separate Category

Safety cars introduce a second problem beyond lap time corruption:
they fundamentally change pit stop decision-making.

Pitting under SC is strategically "free" — the field bunches up behind
the safety car so the driver loses minimal time in the pits relative to
competitors. Teams treat SC periods as reactive opportunistic stops, not
proactive strategy calls.

**Mixing SC pit stops with proactive pit stops in strategy analysis
produces meaningless results.** A team that always pits under SC looks
strategically brilliant — they consistently choose the right tire at the
right time — but the decision was reactive, not analytical.

### Decision for Pit Stop Strategy Mart
Add `pitted_under_sc` boolean column to `mart_pit_strategy`:
```sql
case
    when track_status in ('4', '5') then true
    else false
end as pitted_under_sc
```

All undercut/overcut analysis, strategic timing analysis, and pit
window optimisation filters on `pitted_under_sc = false` — proactive
stops only. SC stops are tracked separately as a distinct strategic
category.

### Lap 1 Exclusion (related)
LapNumber = 1 is excluded from is_rep_lap for similar reasons:
cold tires, maximum fuel load, dirty track surface, and first corner
incident risk make Lap 1 times incomparable to any other lap in the
race.

### dbt Implication
- `is_rep_lap` flag defined once in `stg_lap_times` — covers SC, VSC,
  Lap 1, pit laps, deleted laps, and IsAccurate = False
- `pitted_under_sc` column added to `stg_pit_stops` and surfaced in
  `mart_pit_strategy`
- `mart_tire_degradation` filters `is_rep_lap = true` — SC rest laps
  never appear in degradation curves
- `mart_ml_features` filters `is_rep_lap = true` — SC laps never
  corrupt model training features

## 008 — Yellow Flag Laps: Excluded from is_rep_lap or Not?

### Problem
Yellow flags ('2' in TrackStatus) occur frequently during races — often
multiple times per race. The question is whether yellow flag laps should
be excluded from is_rep_lap the same way SC and VSC laps are.

### Key Difference from SC/VSC
A yellow flag covers one sector only. The driver lifts slightly in that
sector but the rest of the lap is at full pace. Overall lap time impact
is roughly 0.3-0.5 seconds — compared to 30+ seconds for a full SC lap.

Excluding yellow flag laps would remove a significant portion of race
data for a marginal accuracy gain.

### Decision
Yellow flag laps are NOT excluded from is_rep_lap.

Instead a separate boolean column `has_yellow_flag` is added to
stg_lap_times. Downstream models and analysts choose whether to filter
on it depending on the strictness required:

- Strict pace comparison → filter has_yellow_flag = false
- General analysis → keep yellow flag laps, 0.3s noise is acceptable

### Rationale
is_rep_lap excludes laps that are fundamentally incomparable to racing
laps — SC laps, VSC laps, pit laps, Lap 1. A yellow flag lap is still
a racing lap, just slightly compromised in one sector. Treating it the
same as a SC lap would be analytically incorrect and would discard too
much data.

Separating the two gives downstream models the flexibility to be as
strict or as permissive as the analytical question requires.

### Implementation
```python
# is_rep_lap — hard excludes only
laps['is_rep_lap'] = (
    (laps['IsAccurate'] == True) &
    (laps['Deleted'] == False) &
    (laps['LapNumber'] > 1) &
    (~laps['TrackStatus'].astype(str).str.contains('4|5')) &
    (laps['PitInTime'].isna()) &
    (laps['PitOutTime'].isna())
)

# Separate soft flag — analyst's choice to filter
laps['has_yellow_flag'] = (
    laps['TrackStatus'].astype(str).str.contains('2')
)
```

### dbt Implication
Both columns are surfaced in stg_lap_times. mart_driver_season_performance
and mart_ml_features use is_rep_lap only. Any model requiring strict
single-lap pace comparison (e.g. constructor upgrade detection using
qualifying data) additionally filters has_yellow_flag = false.


## 009 — Pit Stop Duration Outlier Filtering

### Problem
Some pit stop records have unrealistic durations — up to 2393 seconds.
These occur when a driver retires in the pit lane or has a mechanical
issue. PitOutTime records when the car eventually left the pit box,
not when a normal stop completed.

### Why Not Hardcode a Threshold
A fixed cutoff is arbitrary and fragile — different eras of F1 have
different average pit stop durations and a 2025 run would need
different values.

### Why Pure IQR Fails
Races with very few pit stops (Monaco, Japan, Brazil) have 2-3 data
points. One extreme outlier makes IQR bounds explode to negative
thousands and positive thousands — completely meaningless for those
races.

### Decision
Combine IQR with physics-based absolute bounds:

- PHYSICAL_MIN = 15 seconds — physically impossible to complete a pit
  stop including pit lane entry and exit in less than this
- PHYSICAL_MAX = 300 seconds — no strategic stop takes 5+ minutes

lower_bound = max(PHYSICAL_MIN, Q1 - 1.5 * IQR)
upper_bound = min(PHYSICAL_MAX, Q3 + 1.5 * IQR)

For normal races: IQR bounds are tighter than physics bounds and
drive the filter. For low-sample races (Monaco, Japan, Brazil): physics
bounds act as a safety net preventing nonsensical IQR bounds.

### Rationale
Two-layer approach — data-driven where sample size allows, physics-
constrained where it doesn't. Neither purely hardcoded nor purely
statistical. Adapts to the data while staying grounded in reality.

### dbt Implication
Filter applied at ingestion in pit_stops.py. stg_pit_stops receives
only valid strategic stops. Retirement stops captured in stg_results
via the Status column.

## 010 — DNF Classification and Status Field Values

### Problem
FastF1 session.results Status field uses different values than expected.
No 'Accident', 'Engine', or 'Collision' values exist — FastF1 simplifies
to five clean statuses.

### Actual FastF1 Status Values (2024 season)
- 'Finished'     — completed race on lead lap (287 occurrences)
- 'Lapped'       — finished race but one or more laps behind leader (138)
- 'Retired'      — did not finish — mechanical or accident (49)
- 'Did not start'— DNS, never left the grid (3)
- 'Disqualified' — completed race but excluded post-race (2)

### Decision
is_dnf = True when Status is 'Retired' or 'Did not start'
Finished, Lapped, and Disqualified drivers completed race distance.

dnf_type cannot be determined from FastF1 results alone.
FastF1 does not distinguish mechanical failures from driver errors.
Detailed retirement reasons (Engine, Accident, Gearbox etc) come from
Ergast API — joined in int_driver_era_unified dbt model.

### Rationale
Lapped drivers are NOT DNFs — they completed the full race distance,
just slower than the leader. Disqualified drivers also completed race
distance — their exclusion is a post-race stewards decision, not a
retirement. Treating either as DNF would incorrectly inflate DNF counts
and corrupt reliability analysis.

### dbt Implication
stg_results exposes is_dnf and dnf_type columns.
int_driver_era_unified joins Ergast retirement reasons to populate
detailed dnf_type (mechanical vs driver) for post-2003 seasons where
Ergast has retirement detail.

---

*Last updated: March 2026*
*Author: Priyal Maniar*
*Project: PitWall — F1 Race Intelligence Pipeline*