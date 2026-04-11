import fastf1
import pandas as pd
import os

from ingestion.config import YEARS, RAW_DIR, RACE_SESSION, get_race_names

def get_laps_with_weather(season: int, race: str) -> pd.DataFrame:
    session = fastf1.get_session(season, race, RACE_SESSION)
    session.load(telemetry=False, weather=True)

    laps = session.laps[['Driver', 'LapNumber', 'Time']].copy()
    weather = session.weather_data.copy()

    laps = laps.sort_values('Time')
    weather = weather.sort_values('Time')

    merged = pd.merge_asof(laps, weather, on='Time', direction='nearest')

    merged['Time'] = merged['Time'].dt.total_seconds()
    merged['Race'] = race
    merged['Year'] = season
    return merged

def extract_all_races(years: list = YEARS) -> pd.DataFrame:
    all_weather = []

    for season in years:
        races = get_race_names(season)
        for race in races:
            print(f"Extracting weather: {season} {race}")
            try:
                weather = get_laps_with_weather(season, race)
                all_weather.append(weather)
            except Exception as e:
                print(f"Error extracting {season} {race}: {e}")
                continue

    return pd.concat(all_weather, ignore_index=True)

def run():
    print("Extracting weather data")
    df = extract_all_races(YEARS)
    os.makedirs(RAW_DIR, exist_ok=True)
    df.to_csv(f'{RAW_DIR}/weather_2023_2025.csv', index=False)
    print(f"Weather extraction complete for {len(df)} laps")

if __name__ == "__main__":
    run()