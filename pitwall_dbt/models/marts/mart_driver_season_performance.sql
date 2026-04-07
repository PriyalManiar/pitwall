with driver_race as (
    select * from {{ ref('int_driver_race_performance') }}
)

select
    driver_code,
    driver_number,
    team,
    year,

    -- Season pace
    avg(avg_rep_lap_seconds)                as season_avg_pace_seconds,
    min(best_lap_seconds)                   as season_best_lap_seconds,
    avg(lap_time_stddev)                    as season_consistency,

    -- Season results
    sum(points)                             as total_points,
    count(*)                                as races_entered,
    count(case when is_dnf then 1 end)      as dnf_count,
    count(case when not is_dnf
        and classified_position = 1
        then 1 end)                         as wins,
    count(case when not is_dnf
        and classified_position <= 3
        then 1 end)                         as podiums,
    count(case when grid_position = 1
        then 1 end)                         as pole_positions,

    -- Positions
    avg(positions_gained)                   as avg_positions_gained,
    sum(positions_gained)                   as total_positions_gained,

    -- Pit stops
    avg(pit_stop_count)                     as avg_pit_stops_per_race,
    avg(avg_pit_duration_seconds)           as avg_pit_duration_seconds,

    -- Points lost to DNF
    sum(case when is_dnf then 0
        else points end)                    as points_scored,
    count(case when is_dnf then 1 end) * 10 as est_points_lost_to_dnf

from driver_race
group by 1, 2, 3, 4