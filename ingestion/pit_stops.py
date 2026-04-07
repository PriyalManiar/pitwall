import fastf1
import pandas as pd
import os

from ingestion.config import RACES_2024, YEAR, RAW_DIR, RACE_SESSION

# Physics-based absolute bounds — no strategic pit stop falls outside these
# Below 15s: physically impossible including pit lane entry and exit
# Above 300s: 5 minutes — no strategic stop takes this long
PHYSICAL_MIN = 15
PHYSICAL_MAX = 300

def get_pit_stops(season: int, race: str) -> pd.DataFrame:
    """
    Reconstruct pit stop events from lap data.
    PitInTime and PitOutTime live on different rows by design —
    self-join on consecutive Stint numbers to reconstruct full event.
    See DECISIONS.md #002 for full rationale.
    Outlier filtering uses IQR + physics bounds — see DECISIONS.md #009.
    """
    session = fastf1.get_session(season, race, RACE_SESSION)
    session.load(telemetry=False, weather=False)

    laps = session.laps[[
        'Driver', 'DriverNumber', 'Team',
        'LapNumber', 'Stint',
        'PitInTime', 'PitOutTime',
        'Compound', 'TrackStatus'
    ]].copy()

    laps['Race'] = race
    laps['Year'] = season

    # Convert timedeltas to float seconds for Snowflake
    for col in ['PitInTime', 'PitOutTime']:
        laps[col] = laps[col].dt.total_seconds()

    #  PitIn side — last lap of each stint 
    pit_in = laps[laps['PitInTime'].notna()][[
        'Driver', 'DriverNumber', 'Team',
        'Race', 'Year',
        'LapNumber', 'Stint',
        'PitInTime', 'Compound', 'TrackStatus'
    ]].copy()

    #  PitOut side — first lap of next stint 
    pit_out = laps[laps['PitOutTime'].notna()][[
        'Driver', 'Race', 'Year',
        'Stint', 'PitOutTime', 'Compound'
    ]].copy()

    # Rename Compound on pit_out — this is the NEW tire after the stop
    pit_out = pit_out.rename(columns={'Compound': 'CompoundNew'})

    #  Self join on consecutive stints 
    # Stint 1 pit_in joins with Stint 2 pit_out
    # Stint 2 pit_in joins with Stint 3 pit_out
    pit_out['Stint'] = pit_out['Stint'] - 1

    pit_stops = pit_in.merge(
        pit_out,
        on=['Driver', 'Race', 'Year', 'Stint'],
        how='inner'
    )

    pit_stops['pit_duration_seconds'] = (
        pit_stops['PitOutTime'] - pit_stops['PitInTime']
    )

    # Was this stop made under safety car or VSC?
    pit_stops['pitted_under_sc'] = (
        pit_stops['TrackStatus'].astype(str).str.contains('4|5')
    )

    pit_stops = pit_stops.drop(columns=['TrackStatus'])

    # ── Outlier filtering — IQR + physics bounds ──────────────────────────────
    # See DECISIONS.md #009 for full rationale
    # Pure IQR fails for low-sample races (Monaco, Japan, Brazil)
    # Physics bounds act as safety net for those cases
    Q1 = pit_stops['pit_duration_seconds'].quantile(0.25)
    Q3 = pit_stops['pit_duration_seconds'].quantile(0.75)
    IQR = Q3 - Q1

    lower_bound = max(PHYSICAL_MIN, Q1 - 1.5 * IQR)
    upper_bound = min(PHYSICAL_MAX, Q3 + 1.5 * IQR)

    pit_stops = pit_stops[
        (pit_stops['pit_duration_seconds'] >= lower_bound) &
        (pit_stops['pit_duration_seconds'] <= upper_bound)
    ]

    print(f"  IQR bounds: {lower_bound:.1f}s — {upper_bound:.1f}s | {len(pit_stops)} stops")

    return pit_stops


def extract_all_races(season: int = YEAR) -> pd.DataFrame:
    all_pit_stops = []

    for race in RACES_2024:
        print(f"  Extracting pit stops: {race}...")
        try:
            df = get_pit_stops(season, race)
            all_pit_stops.append(df)
        except Exception as e:
            print(f"  Faled {race}: {type(e).__name__}: {e}")
            continue

    if not all_pit_stops:
        raise RuntimeError("No races loaded — check errors above")

    return pd.concat(all_pit_stops, ignore_index=True)


def run():
    print("Starting pit stops extraction...")
    df = extract_all_races(YEAR)
    os.makedirs(RAW_DIR, exist_ok=True)
    df.to_csv(f'{RAW_DIR}/pit_stops_{YEAR}.csv', index=False)
    print(f"\n Pit stops extraction complete — {len(df)} stops across {df['Race'].nunique()} races")
    print(f"   Avg duration: {df['pit_duration_seconds'].mean():.1f}s")
    print(f"   Fastest stop: {df['pit_duration_seconds'].min():.1f}s")
    print(f"   Slowest stop: {df['pit_duration_seconds'].max():.1f}s")
    print(f"   SC stops: {df['pitted_under_sc'].sum()} | Clean stops: {(~df['pitted_under_sc']).sum()}")

if __name__ == "__main__":
    run()