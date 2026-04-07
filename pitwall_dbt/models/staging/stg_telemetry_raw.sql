with source as (
    select * from {{ source('raw', 'telemetry_raw') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['driver', 'lapnumber', 'race', 'year', 'distance', 'speed']) }} as telemetry_id,
        driver                  as driver_code,
        lapnumber               as lap_number,
        race,
        year,

        -- Telemetry channels
        distance                as distance_m,
        speed                   as speed_kmh,
        throttle                as throttle_pct,
        brake                   as is_braking,
        x                       as pos_x,
        y                       as pos_y

    from source
)

select * from renamed