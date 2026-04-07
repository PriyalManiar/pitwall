-- Stub: will be completed after Ergast ingestion
-- Unifies FastF1 post-2018 data with Ergast pre-2018 data
-- Adds data_regime flag for Looker filtering

with fastf1_results as (
    select
        driver_code,
        driver_number,
        driver_name,
        team,
        race,
        year,
        grid_position,
        classified_position,
        points,
        status,
        is_dnf,
        'post_2018_fastf1'      as data_regime
    from {{ ref('stg_results') }}
)

-- Ergast CTE will be added here after ingestion
-- union all with pre_2018_ergast

select * from fastf1_results