with source as (
    select * from {{ source('raw', 'pit_stops') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['drivernumber', 'race', 'year', 'stint']) }} as pit_stop_id,

        driver                  as driver_code,
        drivernumber            as driver_number,
        team,
        race,
        year,
        lapnumber               as lap_number,
        stint,

        -- Compounds
        compound                as compound_old,
        compoundnew             as compound_new,

        -- Timing
        pitintime               as pit_in_time_seconds,
        pitouttime              as pit_out_time_seconds,
        pit_duration_seconds,

        -- Flags
        pitted_under_sc

    from source
)

select * from renamed