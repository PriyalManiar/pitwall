with source as (
    select * from {{ source('raw', 'weather') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['driver', 'lapnumber', 'race', 'year']) }} as weather_id,

        driver                  as driver_code,
        lapnumber               as lap_number,
        race,
        year,

        -- Timing
        time                    as time_seconds,

        -- Weather conditions
        airtemp                 as air_temp_c,
        humidity                as humidity_pct,
        pressure                as pressure_mbar,
        rainfall                as is_raining,
        tracktemp               as track_temp_c,
        winddirection           as wind_direction_deg,
        windspeed               as wind_speed_ms

    from source
)

select * from renamed