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

011 — Docker custom image: Built custom Dockerfile FROM apache/airflow:2.9.0 with fastf1 pre-installed.
      Avoids ad-hoc runtime pip installs into containers, which are lost on restart and not reproducible.

012 — CeleryExecutor: Chosen over SequentialExecutor for production realism. Redis as broker enables
      parallel task execution across workers. Demonstrates understanding of distributed task queues.

013 — schedule=None: Pipeline triggers manually or via external trigger. F1 data ingestion is
      event-driven (race weekends), not time-driven. A cron schedule would be semantically incorrect.

014 — run_all.py: Convenience runner for local development and testing outside Airflow.
      Calls the same run() functions the DAG uses — single source of truth, no duplication.


## dbt Layer

**Staging:**
- One model per raw table. Renames columns to snake_case, adds surrogate key via dbt_utils.generate_surrogate_key()
- Materialized as views — no data duplication, just a clean lens over RAW
- surrogate key composition: lap_id (driver_number + lap_number + race + year), pit_stop_id (driver_number + race + year + stint), result_id (driver_number + race + year), weather_id (driver + lap_number + race + year), telemetry_id (driver + lap_number + race + year + distance + speed)

**Intermediate:**
- int_driver_race_performance: joins stg_results + stg_lap_times + stg_pit_stops. Base model for all driver/constructor marts. Uses CASE WHEN to aggregate rep laps only without filtering the full dataset
- int_pit_strategy_outcomes: self-join on lap_times to get position at pit lap vs 3 laps after. Derives strategy_outcome (undercut_success/overcut_success/neutral)
- int_weather_pace_impact: window function to calculate dry baseline pace per driver per race. pace_delta_seconds = actual - baseline
- int_driver_era_unified: stub for FastF1 + Ergast unification. data_regime flag added for Looker filtering

**Marts:**
- Materialized as tables — pre-computed for Looker query performance
- mart_ml_features: one row per lap with all ML input variables. Target variable = pitted_this_lap (binary)
- Race name inconsistency: FastF1 returns shortened country names for lap data (e.g. "Bahrain") but full names for results (e.g. "Bahrain Grand Prix"). Fixed by re-running ingestion — race name is set explicitly from RACES_2024 config, not from FastF1 session metadata

**dbt tests:**
- unique + not_null on all surrogate keys
- Telemetry unique test removed — time series table with no natural unique key. Completeness matters, not row uniqueness
- 225 null lap_time_seconds in stg_lap_times are expected — DNF laps and timing failures

015 — dbt materialization strategy: staging/intermediate as views (no storage cost, always fresh),
      marts as tables (pre-computed for Looker performance). Views recompute on every query which
      is fine for transformation layers but too slow for dashboard queries over 26K+ rows.

016 — Race name standardization: FastF1 returns shortened names for lap sessions vs full names
      for result sessions. Fixed at ingestion layer by explicitly setting Race column from
      RACES_2024 config. dbt is not the right place to fix source data inconsistencies.

018 — Telemetry storage: Raw telemetry (27.2M rows) stored as Parquet instead of CSV.
      Parquet is columnar, compressed, and natively supported by Snowflake COPY INTO.
      5-10x smaller than CSV for the same data. MATCH_BY_COLUMN_NAME handles schema
      mapping automatically. CSV would have required manual column ordering.

019 — Telemetry extraction strategy: Agg telemetry extracted locally (fast, cached).
      Raw telemetry extracted on Google Colab (3 parallel notebooks, one per year,
      separate IPs = separate rate limits). Avoids FastF1 500 calls/hour limit.
      Raw telemetry not cached locally — too large for Mac storage.

020 — ML train/test split by year: Train on 2023-2024, test on 2025. Random split
      would leak same-race laps into both sets — a driver's lap 30 in train and lap
      31 in test from the same race is data leakage. Year-based split simulates real
      production deployment: model trained on history, evaluated on unseen future races.

021 — Pit stop model threshold: Default 0.5 maximizes F1 (0.214). Threshold 0.1
      maximizes recall (0.572) at cost of precision. For live race strategy use case,
      lower threshold preferred — missing a pit window costs positions, false alarms
      are cheap. Kafka consumer uses threshold=0.2 for real-time inference.

022 — Feature engineering in dbt not Python: gap_to_car_ahead, laps_remaining,
      lap_time_delta_3 derived in mart_ml_features.sql. Features available to any
      downstream model or dashboard, documented, tested, version controlled. ML script
      only pulls features — does not engineer them.

023 — dbt tests scope: unique + not_null on surrogate keys. Telemetry unique test
      removed — time series table with no natural unique key, completeness matters
      not row uniqueness. lap_time_seconds not_null removed — DNF laps and timing
      failures produce legitimate nulls. Tests should catch bugs, not flag expected
      data characteristics.

024 — Lap time predictor results: RMSE=2.079s, R²=0.961 on 2025 test set. Model explains
      96.1% of lap time variance using telemetry + weather + tyre features. Max error of
      50.659s attributed to SC/VSC laps slipping through is_rep_lap filter — data quality
      edge case, not model failure. XGBoost outperformed RandomForest and LinearRegression.

025 — Pit stop predictor results: F1=0.213, Precision=0.153, Recall=0.347 on 2025 test set.
      Low precision expected — pit strategy depends on gap to car ahead, competitor tyre age,
      safety car probability — information not fully captured in telemetry. Threshold=0.2
      used for Kafka live inference (recall=0.482) — missing a pit window costs positions,
      false alarms are cheap. XGBoost selected via cross-validation over LogisticRegression
      and RandomForest.

026 — SHAP values: TreeExplainer used for XGBoost and RandomForest models. Summary plots
      saved to ml/plots/. SHAP computed on first 500 test rows for performance. Explains
      individual prediction contributions — interview-ready explanation of model decisions.

027 — Optuna hyperparameter tuning: 50 trials per model using Bayesian optimization.
      Smarter than GridSearchCV which tries all combinations exhaustively. Improved pit
      stop F1 from 0.142 to 0.151 on training set. Direction=maximize for F1 (classification)
      and maximize for neg_MSE (regression).

028 — f1db as Ergast replacement: Ergast API sunset in 2024, Jolpica replacement blocked
      by network. f1db GitHub releases provide same data as CSV — downloaded via curl,
      no API key required. Filtered to pre-2023 (year < 2023) to avoid overlap with
      FastF1 2023-2025 data. 25,892 rows covering 1950-2022, 73 seasons.

029 — Data coverage strategy: f1db 1950-2022 for historical champions dashboard.
      FastF1 2023-2025 for ML training (same regulation era, consistent patterns).
      Kafka/OpenF1 2026 for live inference. Gap years avoided by design — 2022 was
      new regulation era, mixing pre/post 2022 data would introduce noise.

030 — Telemetry raw extraction via Colab: 3 parallel Colab notebooks (one per year),
      each with separate IP = separate 500 calls/hour rate limit. Cache stored in
      Google Drive per notebook. 27.2M rows across 70 races extracted successfully.
      2023 Emilia Romagna missing — race was cancelled due to flooding, not a data error.

031 — Lap time predictor overfitting fix: lap_time_delta_3_seconds removed from
      lap time predictor features — it's derived from the target variable (lap_time_seconds)
      creating target leakage. Model learns to predict current lap time from recent lap times
      rather than from causal features. Removed from lap time model only — retained in pit
      stop model where target is pitted_this_lap (different variable, no leakage).

032 — Lap time predictor error analysis: RMSE of 5.3s is misleading due to 42 extreme
      outlier rows (0.06% of data). Error distribution: 61.5% within 2s, 86.3% within 5s,
      99.94% within 20s. Outliers are likely VSC/SC laps not caught by is_rep_lap filter
      or telemetry sensor anomalies. Median error is more representative metric than RMSE
      for this dataset. RMSE penalizes large errors quadratically — one 30s error contributes
      as much as 225 one-second errors.

033 — Lap time predictor improvement: First run achieved RMSE=2.079s, R²=0.961 before
      weather join fix. After fixing driver_code join in mart_ml_features (was joining on
      driver_number = driver_code which is wrong), weather data fully populated. Model
      performance changed due to different data distribution — trade-off between data
      quality and model performance accepted. Final model: RMSE=5.3s, R²=0.741, but
      86.3% of predictions within 5s which is operationally acceptable for strategy use.

034 — race and driver_code added to MART_LAP_PREDICTIONS: Required for error analysis
      by circuit and driver. Enables Looker to slice prediction errors by race/driver.
      Not added to MART_PIT_PREDICTIONS initially — will be added if needed for dashboards.

## Kafka Setup

**Decision:** KRaft mode (no Zookeeper)
**Why:** Zookeeper is legacy as of Kafka 2.8+. KRaft lets the broker manage its own metadata. One fewer service to run and maintain.

**Decision:** Single broker, single partition, replication factor 1
**Why:** Portfolio project. Production would use 3+ brokers with replication for fault tolerance. Over-engineering this adds operational complexity with no interview benefit.

**Decision:** Two listeners (PLAINTEXT on 9092, CONTROLLER on 9093)
**Why:** Kafka needs to be reachable from two contexts — inside Docker (container-to-container) and from the Mac (producer/consumer scripts). Single listener can't serve both.

**Decision:** Producer and consumer run on host, broker runs in Docker
**Why:** Broker is a persistent service — Docker is the right home. Producer/consumer are scripts — keeping them on the host makes iteration and debugging faster during development. In production, all three would be containerized.

**Topic:** pitwall.live.telemetry
**Why:** Dot-separated naming convention signals hierarchy — project.environment.data_type. Standard Kafka naming pattern.


## Kafka Producer

**Decision:** Replay mode using OpenF1 historical session data
**Why:** Enables on-demand demo without needing a live race weekend. Same producer code points at live OpenF1 endpoints during an actual race — only the session key changes.

**Decision:** Sort all driver laps by date_start before publishing
**Why:** Interleaves drivers chronologically, mimicking how a real race feed would arrive — not all laps for driver 1, then all for driver 4.

**Decision:** 4-second sleep between messages
**Why:** Matches OpenF1's live polling interval. Realistic cadence without overwhelming the consumer during development.

**Decision:** Filter to only model-required fields before publishing
**Why:** Keeps messages lean. No point streaming segments_sector data we never use in inference.

## Kafka Consumer

**Decision:** Feature store pattern instead of per-message API calls
**Why:** Enriching each Kafka message with 4 additional API calls (stints, position, weather, car data) would hit OpenF1's 30 requests/minute rate limit mid-race. A feature store pre-fetches all enrichment data once at startup and stores it in memory — instant lookup per message, no rate limit risk. This is the standard pattern used in production ML systems.

**Decision:** In-memory feature store keyed by (driver_number, lap_number)
**Why:** Snowflake would be too slow for per-message lookups during live inference. Python dictionary lookup is O(1) — no network round trip, no query latency. Race strategy decisions need to be made in seconds, not minutes.

**Decision:** Feature store cached to disk after first build
**Why:** Building the feature store requires ~80 API calls and takes 7 minutes due to rate limiting. Caching to pickle after first build means subsequent runs load in under 1 second. Cache is invalidated manually when switching sessions.

**Decision:** Real gap data from OpenF1 intervals endpoint
**Why:** gap_to_car_ahead_seconds is the strongest feature in the pit stop model. Hardcoding 0.0 would make every driver look like they're right behind the car ahead — inflating pit probabilities falsely. OpenF1 intervals endpoint provides real-time gap data timestamped to match lap start times.

**Decision:** Lap 1 and 2 excluded from inference (lap_number < 3)
**Why:** Interval data is unreliable in the first two laps — the field is still bunched from the start, gaps haven't stabilized, and tyre temperatures haven't reached operating window. Model predictions on lap 1-2 produce false positives. By lap 3 the field has settled into race pace and gaps are meaningful.

**Decision:** Retired drivers filtered out in producer (fewer than 20 laps)
**Why:** Drivers who retire early have very low laps_remaining from lap 1, which the model interprets as late-race urgency and flags as PIT NOW. Filtering retired drivers at the producer level prevents misleading inference on drivers who never completed a full race.

**Decision:** Both pit stop and lap time models run per message
**Why:** Pit probability drives strategy decisions (when to pit). Predicted lap time provides context (how fast the car is currently running). Both written to MART_PIT_PREDICTIONS_LIVE for Tableau dashboard consumption. Running both models adds negligible latency per message.

**Decision:** DataFrame passed to models instead of raw numpy array
**Why:** RandomForestRegressor was trained with named feature columns. Passing a plain numpy array triggers sklearn warnings and risks silent feature misalignment. DataFrame preserves column names — model receives features in the exact order it was trained on.

**Decision:** threshold=0.2 for live pit stop inference
**Why:** Model precision is low (0.153) but recall at 0.2 threshold is 0.482. In live race strategy, missing a pit window costs track position — false alarms are cheap (strategist ignores it), missed windows are expensive (driver loses position). Higher recall justified by asymmetric cost of errors.

---


*Last updated: April 2026*
*Author: Priyal Maniar*
*Project: PitWall — F1 Race Intelligence*
