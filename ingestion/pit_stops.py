import fastf1
import pandas as pd
import os
import numpy as np

from ingestion.config import YEARS, RAW_DIR, RACE_SESSION, get_race_names

PHYSICAL_MIN = 15
PHYSICAL_MAX = 300

def get_pit_stops(season: int, race: str) -> pd.DataFrame:
    session = fastf1.get_session(season, race, RACE_SESSION)
    session.load(telemetry=False, weather=False)

    laps = session.laps[[
        'Driver', 'DriverNumber', 'Team', 'LapNumber', 'Stint',
        'PitInTime', 'PitOutTime', 'Compound', 'TrackStatus'
    ]].copy()

    laps['Race'] = race
    laps['Year'] = season

    for col in ['PitInTime', 'PitOutTime']:
        laps[col] = laps[col].dt.total_seconds()

    pit_in = laps[laps['PitInTime'].notna()][
        ['Driver', 'DriverNumber', 'Team', 'Race', 'Year', 'LapNumber', 'Stint', 'PitInTime', 'Compound', 'TrackStatus']
    ].copy()

    pit_out = laps[laps['PitOutTime'].notna()][
        ['Driver', 'DriverNumber', 'Team', 'Race', 'Year', 'LapNumber', 'Stint', 'PitOutTime', 'Compound']
    ].copy()
    pit_out = pit_out.rename(columns={'Compound': 'CompoundNew'})
    pit_out['Stint'] = pit_out['Stint'] - 1

    pit_stops = pit_in.merge(
        pit_out[['Driver', 'Race', 'Year', 'Stint', 'PitOutTime', 'CompoundNew']],
        on=['Driver', 'Race', 'Year', 'Stint']
    )

    pit_stops['pit_duration_seconds'] = pit_stops['PitOutTime'] - pit_stops['PitInTime']

    pit_stops['pitted_under_sc'] = (
        pit_stops['TrackStatus'].astype(str).str.contains('4|5')
    )

    q1 = pit_stops['pit_duration_seconds'].quantile(0.25)
    q3 = pit_stops['pit_duration_seconds'].quantile(0.75)
    iqr = q3 - q1
    lower = max(q1 - 1.5 * iqr, PHYSICAL_MIN)
    upper = min(q3 + 1.5 * iqr, PHYSICAL_MAX)

    pit_stops = pit_stops[
        (pit_stops['pit_duration_seconds'] >= lower) &
        (pit_stops['pit_duration_seconds'] <= upper)
    ]

    return pit_stops

def extract_all_races(years: list = YEARS) -> pd.DataFrame:
    all_stops = []

    for season in years:
        races = get_race_names(season)
        for race in races:
            print(f"Extracting pit stops: {season} {race}")
            try:
                stops = get_pit_stops(season, race)
                all_stops.append(stops)
            except Exception as e:
                print(f"Error extracting {season} {race}: {e}")
                continue

    return pd.concat(all_stops, ignore_index=True)

def run():
    print("Extracting pit stops")
    df = extract_all_races(YEARS)
    os.makedirs(RAW_DIR, exist_ok=True)
    df.to_csv(f'{RAW_DIR}/pit_stops_2023_2025.csv', index=False)
    print(f"Pit stops extraction complete for {len(df)} stops")

if __name__ == "__main__":
    run()