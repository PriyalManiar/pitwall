with lap_times as (
    select * from {{ ref('stg_lap_times') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
),

joined as (
    select
        l.lap_id,
        l.driver_code,
        l.driver_number,
        l.team,
        l.race,
        l.year,
        l.lap_number,
        l.lap_time_seconds,
        l.compound,
        l.tyre_life,
        l.is_rep_lap,

        -- Weather at this lap
        w.air_temp_c,
        w.track_temp_c,
        w.humidity_pct,
        w.is_raining,
        w.wind_speed_ms

    from lap_times l
    left join weather w
        on l.driver_number = w.driver_code
        and l.lap_number = w.lap_number
        and l.race = w.race
        and l.year = w.year
),

-- Calculate pace delta vs dry baseline per driver per race
with_baseline as (
    select
        *,
        avg(case when is_rep_lap and not is_raining
            then lap_time_seconds end)
            over (partition by driver_number, race, year) as dry_baseline_seconds,

        lap_time_seconds - avg(case when is_rep_lap and not is_raining
            then lap_time_seconds end)
            over (partition by driver_number, race, year) as pace_delta_seconds

    from joined
)

select * from with_baseline