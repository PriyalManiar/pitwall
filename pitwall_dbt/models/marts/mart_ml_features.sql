with laps as (
    select * from {{ ref('stg_lap_times') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
),

pit_stops as (
    select * from {{ ref('stg_pit_stops') }}
),

telemetry as (
    select
        driver_code,
        lap_number,
        race,
        year,
        avg(speed_kmh)                      as avg_speed,
        max(speed_kmh)                      as max_speed,
        avg(throttle_pct)                   as avg_throttle_pct,
        sum(case when is_braking
            then 1 else 0 end) * 1.0
            / nullif(count(*), 0)           as heavy_braking_pct
    from {{ ref('stg_telemetry_raw') }}
    group by 1, 2, 3, 4
),

pit_flags as (
    select
        driver_number,
        race,
        year,
        lap_number,
        1                                   as pitted_this_lap
    from pit_stops
),

-- Total laps per race for laps_remaining calculation
race_laps as (
    select
        race,
        year,
        max(lap_number)                     as total_laps
    from laps
    group by 1, 2
),

enriched as (
    select
        l.lap_id,
        l.driver_code,
        l.driver_number,
        l.team,
        l.race,
        l.year,
        l.lap_number,

        -- Tyre features
        l.compound,
        l.tyre_life,
        l.is_fresh_tyre,
        l.stint,

        -- Lap context
        l.position,
        l.lap_time_seconds,
        l.is_rep_lap,

        -- Laps remaining — proxy for pit window urgency
        rl.total_laps - l.lap_number       as laps_remaining,

        -- Gap to car ahead — key strategic signal
        l.lap_time_seconds - lag(l.lap_time_seconds)
            over (
                partition by l.race, l.year, l.lap_number
                order by l.position
            )                               as gap_to_car_ahead_seconds,

        -- Pace degradation — lap time delta vs 3 laps ago
        l.lap_time_seconds - lag(l.lap_time_seconds, 3)
            over (
                partition by l.driver_code, l.race, l.year
                order by l.lap_number
            )                               as lap_time_delta_3_seconds,

        -- Weather features
        w.track_temp_c,
        w.air_temp_c,
        w.is_raining,
        w.humidity_pct,

        -- Telemetry features
        t.avg_speed,
        t.max_speed,
        t.avg_throttle_pct,
        t.heavy_braking_pct,

        -- Target variable
        coalesce(pf.pitted_this_lap, 0)     as pitted_this_lap

    from laps l
    left join weather w
        on l.driver_code = w.driver_code
        and l.lap_number = w.lap_number
        and l.race = w.race
        and l.year = w.year
    left join telemetry t
        on l.driver_code = t.driver_code
        and l.lap_number = t.lap_number
        and l.race = t.race
        and l.year = t.year
    left join pit_flags pf
        on l.driver_number = pf.driver_number
        and l.lap_number = pf.lap_number
        and l.race = pf.race
        and l.year = pf.year
    left join race_laps rl
        on l.race = rl.race
        and l.year = rl.year
)

select * from enriched