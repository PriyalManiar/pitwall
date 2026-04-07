with strategy as (
    select * from {{ ref('int_pit_strategy_outcomes') }}
)

select
    team,
    race,
    year,
    compound_old,
    compound_new,

    -- Stop timing
    avg(pit_duration_seconds)               as avg_pit_duration_seconds,
    min(pit_duration_seconds)               as fastest_pit_seconds,
    count(*)                                as total_stops,

    -- Strategy outcomes
    count(case when strategy_outcome = 'undercut_success'
        then 1 end)                         as undercut_successes,
    count(case when strategy_outcome = 'overcut_success'
        then 1 end)                         as overcut_successes,
    count(case when strategy_outcome = 'neutral'
        then 1 end)                         as neutral_outcomes,

    -- Average positions gained after pit
    avg(positions_gained_after_pit)         as avg_positions_gained,

    -- SC pit stops
    sum(case when pitted_under_sc
        then 1 else 0 end)                  as sc_pit_count

from strategy
group by 1, 2, 3, 4, 5