with lap_times as (
    select * from {{ ref('stg_lap_times') }}
),

results as (
    select * from {{ ref('stg_results') }}
),

pit_stops as (
    select * from {{ ref('stg_pit_stops') }}
),

-- Aggregate lap metrics per driver per race
lap_metrics as (
    select
        driver_code,
        driver_number,
        team,
        race,
        year,

        -- Pace metrics (rep laps only)
        avg(case when is_rep_lap then lap_time_seconds end)     as avg_rep_lap_seconds,
        min(case when is_rep_lap then lap_time_seconds end)     as best_lap_seconds,
        stddev(case when is_rep_lap then lap_time_seconds end)  as lap_time_stddev,
        count(case when is_rep_lap then 1 end)                  as rep_lap_count,
        count(*)                                                as total_laps,
        count(case when has_yellow_flag then 1 end)             as yellow_flag_laps

    from lap_times
    group by 1, 2, 3, 4, 5
),

-- Aggregate pit stop metrics per driver per race
pit_metrics as (
    select
        driver_number,
        race,
        year,
        count(*)                            as pit_stop_count,
        avg(pit_duration_seconds)           as avg_pit_duration_seconds,
        listagg(compound_old, ' -> ')
            within group (order by stint)   as compound_sequence,
        count(case when pitted_under_sc
            then 1 end)                     as sc_pit_count

    from pit_stops
    group by 1, 2, 3
)

select
    -- Identifiers
    r.result_id,
    lm.driver_code,
    lm.driver_number,
    lm.team,
    lm.race,
    lm.year,

    -- Race result
    r.grid_position,
    r.classified_position,
    r.position,
    r.positions_gained,
    r.points,
    r.status,
    r.is_dnf,
    r.dnf_type,

    -- Pace
    lm.avg_rep_lap_seconds,
    lm.best_lap_seconds,
    lm.lap_time_stddev,
    lm.rep_lap_count,
    lm.total_laps,
    lm.yellow_flag_laps,

    -- Pit stops
    coalesce(pm.pit_stop_count, 0)          as pit_stop_count,
    pm.avg_pit_duration_seconds,
    pm.compound_sequence,
    coalesce(pm.sc_pit_count, 0)            as sc_pit_count

from results r
left join lap_metrics lm
    on r.driver_number = lm.driver_number
    and r.race = lm.race
    and r.year = lm.year
left join pit_metrics pm
    on r.driver_number = pm.driver_number
    and r.race = pm.race
    and r.year = pm.year