import requests
import pandas as pd
import os
from ingestion.config import YEAR, RAW_DIR

JOLPICA_BASE = 'http://api.jolpi.ca/ergast/f1'

# Pre-2018 seasons only — post-2018 covered by FastF1
ERGAST_YEARS = list(range(1950, 2018))

def get_race_results(season: int) -> pd.DataFrame:
    """
    Fetch all race results for a given season from Jolpica API.
    Returns one row per driver per race.
    """
    url = f'{JOLPICA_BASE}/{season}/results.json?limit=1000'
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()
    races = data['MRData']['RaceTable']['Races']

    rows = []
    for race in races:
        race_name = race['raceName']
        for result in race['Results']:
            rows.append({
                'season':               season,
                'race':                 race_name,
                'round':                int(race['round']),
                'driver_code':          result['Driver'].get('code', ''),
                'driver_number':        result['Driver'].get('permanentNumber', ''),
                'driver_name':          f"{result['Driver']['givenName']} {result['Driver']['familyName']}",
                'team':                 result['Constructor']['name'],
                'grid_position':        int(result['grid']) if result['grid'] else None,
                'classified_position':  result['position'],
                'points':               float(result['points']),
                'status':               result['status'],
                'is_dnf':               result['status'] not in ['Finished', '+1 Lap', '+2 Laps', '+3 Laps', '+4 Laps', '+5 Laps'],
                'dnf_type':             'mechanical' if result['status'] not in ['Finished', '+1 Lap', '+2 Laps', '+3 Laps', '+4 Laps', '+5 Laps'] else None,
                'data_regime':          'pre_2018_ergast'
            })

    return pd.DataFrame(rows)

def extract_all_seasons(start: int = 1950, end: int = 2017) -> pd.DataFrame:
    all_results = []

    for season in range(start, end + 1):
        print(f"Extracting Ergast: {season}")
        try:
            df = get_race_results(season)
            all_results.append(df)
        except Exception as e:
            print(f"Error extracting {season}: {e}")
            continue

    return pd.concat(all_results, ignore_index=True)

def run():
    print("Extracting Ergast historical results (1950-2017)...")
    df = extract_all_seasons()
    os.makedirs(RAW_DIR, exist_ok=True)
    df.to_csv(f'{RAW_DIR}/ergast_results.csv', index=False)
    print(f"Ergast extraction complete: {len(df)} rows, {df['season'].nunique()} seasons")

if __name__ == "__main__":
    run()