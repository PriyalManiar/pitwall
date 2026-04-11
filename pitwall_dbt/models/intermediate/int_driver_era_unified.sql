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
),

f1db_results as (
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
        data_regime
    from {{ ref('stg_f1db_results') }}
)

select * from fastf1_results
union all
select * from f1db_results