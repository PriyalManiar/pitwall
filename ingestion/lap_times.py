import fastf1
import pandas as pd
import os

from ingestion.config import YEARS, RAW_DIR, RACE_SESSION, get_race_names

def get_lap_times(season: int, race: str) -> pd.DataFrame:
    session = fastf1.get_session(season, race, RACE_SESSION)
    session.load(telemetry=False, weather=False)

    laps = session.laps[[
        'Driver', 'DriverNumber', 'Team', 'LapNumber', 'LapTime',
        'Sector1Time', 'Sector2Time', 'Sector3Time',
        'Compound', 'Stint', 'TyreLife', 'FreshTyre', 'Position',
        'PitInTime', 'PitOutTime', 'IsAccurate', 'Deleted', 'TrackStatus'
    ]].copy()

    laps['Race'] = race
    laps['Year'] = season

    for col in ['LapTime', 'Sector1Time', 'Sector2Time', 'Sector3Time', 'PitInTime', 'PitOutTime']:
        laps[col] = laps[col].dt.total_seconds()

    laps['is_rep_lap'] = (
        (laps['IsAccurate'] == True) &
        (laps['Deleted'] == False) &
        (laps['LapNumber'] > 1) &
        (~laps['TrackStatus'].astype(str).str.contains('4|5')) &
        (laps['PitInTime'].isna()) &
        (laps['PitOutTime'].isna())
    )

    laps['has_yellow_flag'] = (
        laps['TrackStatus'].astype(str).str.contains('2')
    )
    return laps

def extract_all_races(years: list = YEARS) -> pd.DataFrame:
    all_laps = []

    for season in years:
        races = get_race_names(season)
        for race in races:
            print(f"Extracting lap times: {season} {race}")
            try:
                laps = get_lap_times(season, race)
                all_laps.append(laps)
            except Exception as e:
                print(f"Error extracting {season} {race}: {e}")
                continue

    return pd.concat(all_laps, ignore_index=True)

def run():
    print("Extracting lap times")
    df = extract_all_races(YEARS)
    os.makedirs(RAW_DIR, exist_ok=True)
    df.to_csv(f'{RAW_DIR}/lap_times_2023_2025.csv', index=False)
    print(f"Lap times extraction complete for {len(df)} laps")

if __name__ == "__main__":
    run()