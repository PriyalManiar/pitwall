with driver_race as (
    select * from {{ ref('int_driver_race_performance') }}
),

constructor_race as (
    select
        team,
        race,
        year,
        sum(points)                             as team_points,
        avg(avg_rep_lap_seconds)                as team_avg_pace,
        min(best_lap_seconds)                   as team_best_lap,
        count(case when is_dnf then 1 end)      as team_dnf_count,

        -- Teammate pace gap
        max(avg_rep_lap_seconds)
            - min(avg_rep_lap_seconds)          as teammate_pace_gap_seconds,

        -- Positions gained by team
        sum(positions_gained)                   as team_positions_gained

    from driver_race
    group by 1, 2, 3
)

select
    team,
    year,
    sum(team_points)                        as total_points,
    avg(team_avg_pace)                      as season_avg_pace,
    min(team_best_lap)                      as season_best_lap,
    sum(team_dnf_count)                     as total_dnfs,
    avg(teammate_pace_gap_seconds)          as avg_teammate_pace_gap,
    sum(team_positions_gained)              as total_positions_gained,
    count(*)                                as races_entered

from constructor_race
group by 1, 2