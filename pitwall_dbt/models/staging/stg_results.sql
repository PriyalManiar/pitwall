with source as (
    select * from {{ source('raw', 'results') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['drivernumber', 'race', 'year']) }} as result_id,

        drivernumber            as driver_number,
        abbreviation            as driver_code,
        fullname                as driver_name,
        teamname                as team,
        race,
        year,

        -- Positions
        gridposition            as grid_position,
        position,
        classifiedposition      as classified_position,
        positions_gained,

        -- Result
        points,
        status,
        time                    as race_time_seconds,

        -- Flags
        is_dnf,
        dnf_type

    from source
)

select * from renamed