with era_unified as (
    select * from {{ ref('int_driver_era_unified') }}
),

season_results as (
    select
        driver_code,
        driver_name,
        year,
        data_regime,
        sum(points)                         as total_points,
        count(*)                            as races,
        count(case when classified_position = 1
            then 1 end)                     as wins,
        count(case when classified_position <= 3
            then 1 end)                     as podiums,
        count(case when grid_position = 1
            then 1 end)                     as poles,
        count(case when is_dnf
            then 1 end)                     as dnfs,
        max(total_points)
            over (partition by year)        as champion_points

    from era_unified
    group by 1, 2, 3, 4
)

select
    *,
    -- Dominance score: win rate * points share
    (wins * 1.0 / races)
        * (total_points * 1.0 / nullif(champion_points, 0))
                                            as dominance_score,
    wins * 1.0 / nullif(poles, 0)          as poles_to_wins_ratio,
    wins * 1.0 / nullif(races, 0)          as win_rate

from season_results