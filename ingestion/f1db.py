import pandas as pd
import os
from ingestion.config import RAW_DIR

F1DB_RACE_RESULTS = '/tmp/f1db-csv/f1db-races-race-results.csv'
F1DB_DRIVERS = '/tmp/f1db-csv/f1db-drivers.csv'
F1DB_RACES = '/tmp/f1db-csv/f1db-races.csv'

def process_historical_results() -> pd.DataFrame:
    results = pd.read_csv(F1DB_RACE_RESULTS, low_memory=False)
    drivers = pd.read_csv(F1DB_DRIVERS)[['id', 'name', 'abbreviation']]
    races = pd.read_csv(F1DB_RACES)[['id', 'officialName']]

    # Filter to get results from 1950 to 2023
    results = results[results['year'] < 2023].copy()

    # Join driver names
    results = results.merge(drivers, left_on='driverId', right_on='id', how='left')

    # Join race names
    results = results.merge(races, left_on='raceId', right_on='id', how='left')

    df = pd.DataFrame({
        'DriverNumber':         results['driverNumber'].astype(str),
        'Abbreviation':         results['abbreviation'],
        'FullName':             results['name'],
        'TeamName':             results['constructorId'],
        'GridPosition':         pd.to_numeric(results['gridPositionNumber'], errors='coerce'),
        'Position':             pd.to_numeric(results['positionNumber'], errors='coerce'),
        'ClassifiedPosition':   pd.to_numeric(results['positionNumber'], errors='coerce'),
        'Points':               results['points'],
        'Status':               results['reasonRetired'].fillna('Finished'),
        'Time':                 results['timeMillis'] / 1000,
        'Race':                 results['officialName'],
        'Year':                 results['year'],
        'positions_gained':     results['positionsGained'],
        'is_dnf':               results['reasonRetired'].notna(),
        'dnf_type':             results['reasonRetired'].apply(
                                    lambda x: 'mechanical' if pd.notna(x) else None),
        'data_regime':          'pre_2018_f1db'
    })

    return df

def run():
    print("Processing f1db historical results (1950-2023)...")
    df = process_historical_results()
    os.makedirs(RAW_DIR, exist_ok=True)
    df.to_csv(f'{RAW_DIR}/f1db_results.csv', index=False)
    print(f"f1db extraction complete: {len(df)} rows, {df['Year'].nunique()} seasons")

if __name__ == "__main__":
    run()