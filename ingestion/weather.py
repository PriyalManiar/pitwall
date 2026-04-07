import fastf1
import pandas as pd
import os

from ingestion.config import RACES_2024, YEAR, RAW_DIR, RACE_SESSION

def get_laps_with_weather(season: int, race : str) -> pd.DataFrame:
    """
    Join weather data to laps data using AsOf join on time.
    """

    session = fastf1.get_session(season,race, RACE_SESSION)
    session.load(telemetry= False, weather=True)

    laps = session.laps[['Driver','LapNumber', 'Time']].copy()
    weather = session.weather_data.copy()

    #Both must be sorted by Time before joining
    laps = laps.sort_values('Time')
    weather = weather.sort_values('Time')

    #AsOf join - each lap gets closest weather reading based on timestamp
    merged = pd.merge_asof(laps, weather, on='Time', direction='nearest')

    #Conver time to seconds after join
    merged['Time'] = merged['Time'].dt.total_seconds()

    merged['Race'] = race
    merged['Year'] = season

    return merged

def extract_all_races (season: int = YEAR) -> pd.DataFrame:
    all_weather = []

    for race in RACES_2024:
        print(f"Extracting weather: {race}")
        try:
            weather = get_laps_with_weather(season, race)
            all_weather.append(weather)
        except Exception as e:
            print(f"Error extracting {race}: {e}")
            continue
    return pd.concat(all_weather, ignore_index=True)

def run():
    print("Extracting weather data")
    df = extract_all_races(YEAR)
    os.makedirs(RAW_DIR, exist_ok=True)
    df.to_csv(f'{RAW_DIR}/weather_{YEAR}.csv', index=False)
    print(f"Weather extraction complete for {len(df)} laps")
    
if __name__ == "__main__":
    run()