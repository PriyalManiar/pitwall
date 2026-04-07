with pit_stops as (
    select * from {{ ref('stg_pit_stops') }}
),

lap_times as (
    select * from {{ ref('stg_lap_times') }}
),

-- Get position at pit lap and 3 laps after
pit_positions as (
    select
        p.pit_stop_id,
        p.driver_code,
        p.driver_number,
        p.team,
        p.race,
        p.year,
        p.lap_number,
        p.stint,
        p.compound_old,
        p.compound_new,
        p.pit_duration_seconds,
        p.pitted_under_sc,

        -- Position at pit lap
        l_in.position                       as position_at_pit,

        -- Position 3 laps after pit
        l_out.position                      as position_3_laps_after,

        -- Position change after pit
        l_in.position - l_out.position      as positions_gained_after_pit,

        -- Lap times before and after pit (pace delta)
        l_in.lap_time_seconds               as lap_time_before_pit,
        l_out.lap_time_seconds              as lap_time_3_laps_after

    from pit_stops p
    left join lap_times l_in
        on p.driver_number = l_in.driver_number
        and p.race = l_in.race
        and p.year = l_in.year
        and p.lap_number = l_in.lap_number
    left join lap_times l_out
        on p.driver_number = l_out.driver_number
        and p.race = l_out.race
        and p.year = l_out.year
        and p.lap_number + 3 = l_out.lap_number
)

select
    *,
    case
        when positions_gained_after_pit > 0 then 'undercut_success'
        when positions_gained_after_pit < 0 then 'overcut_success'
        when positions_gained_after_pit = 0 then 'neutral'
        else 'unknown'
    end as strategy_outcome

from pit_positions