import fastf1
import pandas as pd
import os

from ingestion.config import YEARS, RAW_DIR, RACE_SESSION, get_race_names

def get_results(season: int, race: str) -> pd.DataFrame:
    session = fastf1.get_session(season, race, RACE_SESSION)
    session.load(telemetry=False, weather=False)

    results = session.results[[
        'DriverNumber', 'Abbreviation', 'FullName', 'TeamName',
        'GridPosition', 'Position', 'ClassifiedPosition', 'Points', 'Status', 'Time'
    ]].copy()

    results['Race'] = race
    results['Year'] = season

    results['Time'] = results['Time'].dt.total_seconds()

    for col in ['GridPosition', 'ClassifiedPosition', 'Position']:
        results[col] = pd.to_numeric(results[col], errors='coerce')

    results['positions_gained'] = results['GridPosition'] - results['ClassifiedPosition']
    results['is_dnf'] = results['Status'].isin(['Retired', 'Did not start'])
    results['dnf_type'] = results['is_dnf'].apply(
        lambda x: 'mechanical' if x else None
    )

    return results

def extract_all_races(years: list = YEARS) -> pd.DataFrame:
    all_results = []

    for season in years:
        races = get_race_names(season)
        for race in races:
            print(f"Extracting results: {season} {race}")
            try:
                results = get_results(season, race)
                all_results.append(results)
            except Exception as e:
                print(f"Error extracting {season} {race}: {e}")
                continue

    return pd.concat(all_results, ignore_index=True)

def run():
    print("Extracting results")
    df = extract_all_races(YEARS)
    os.makedirs(RAW_DIR, exist_ok=True)
    df.to_csv(f'{RAW_DIR}/results_2023_2025.csv', index=False)
    print(f"Results extraction complete for {len(df)} records")

if __name__ == "__main__":
    run()