with source as (
    select * from {{ source('raw', 'lap_times') }}
),

renamed as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['drivernumber', 'lapnumber', 'race', 'year']) }} as lap_id,

        -- Identifiers
        driver                  as driver_code,
        drivernumber            as driver_number,
        team,
        race,
        year,

        -- Lap info
        lapnumber               as lap_number,
        stint,
        tyrelife                as tyre_life,
        compound,
        freshtyre               as is_fresh_tyre,
        position,

        -- Timing (all in seconds)
        laptime                 as lap_time_seconds,
        sector1time             as sector_1_seconds,
        sector2time             as sector_2_seconds,
        sector3time             as sector_3_seconds,
        pitintime               as pit_in_time_seconds,
        pitouttime              as pit_out_time_seconds,

        -- Status flags
        trackstatus             as track_status,
        isaccurate              as is_accurate,
        deleted                 as is_deleted,
        is_rep_lap,
        has_yellow_flag

    from source
)

select * from renamed