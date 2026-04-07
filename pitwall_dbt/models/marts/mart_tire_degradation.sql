with lap_times as (
    select * from {{ ref('stg_lap_times') }}
),

telemetry as (
    select
        driver_code,
        lap_number,
        race,
        year,
        avg(speed_kmh)                      as avg_speed,
        avg(throttle_pct)                   as avg_throttle,
        sum(case when is_braking
            then 1 end) * 1.0
            / count(*)                      as braking_intensity
    from {{ ref('stg_telemetry_raw') }}
    group by 1, 2, 3, 4
)

select
    l.driver_code,
    l.team,
    l.race,
    l.year,
    l.compound,
    l.tyre_life,
    l.stint,
    l.lap_time_seconds,
    l.is_rep_lap,

    -- Degradation proxy: lap time delta vs first lap of stint
    l.lap_time_seconds - min(l.lap_time_seconds)
        over (partition by l.driver_code, l.race, l.year, l.stint)
                                            as deg_seconds,

    -- Telemetry context
    t.avg_speed,
    t.avg_throttle,
    t.braking_intensity

from lap_times l
left join telemetry t
    on l.driver_code = t.driver_code
    and l.lap_number = t.lap_number
    and l.race = t.race
    and l.year = t.year