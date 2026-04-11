import fastf1
import pandas as pd
import os

from ingestion.config import YEARS, RAW_DIR, RACE_SESSION, get_race_names

def get_telemetry(season: int, race: str):
    session = fastf1.get_session(season, race, RACE_SESSION)
    session.load(telemetry=True, weather=False)

    agg_rows = []

    for drv in session.drivers:
        try:
            drv_laps = session.laps.pick_drivers(drv)
            for _, lap in drv_laps.iterlaps():
                tel = lap.get_telemetry()
                tel = tel[tel['Source'] == 'car']

                if tel.empty:
                    continue

                agg_rows.append({
                    'Driver': lap['Driver'],
                    'LapNumber': lap['LapNumber'],
                    'Race': race,
                    'Year': season,
                    'max_speed': tel['Speed'].max(),
                    'min_speed': tel['Speed'].min(),
                    'avg_speed': tel['Speed'].mean(),
                    'full_throttle_pct': (tel['Throttle'] == 100).mean() * 100,
                    'heavy_braking_pct': tel['Brake'].mean() * 100,
                    'drs_open_pct': (tel['DRS'] >= 10).mean() * 100 if 'DRS' in tel.columns else None
                })

        except Exception as e:
            continue

    tel_agg = pd.DataFrame(agg_rows)
    print(f"  Agg rows: {len(tel_agg)}")
    return tel_agg

def extract_all_races(years: list = YEARS):
    all_agg = []

    for season in years:
        races = get_race_names(season)
        season_agg = []

        for race in races:
            print(f"Extracting telemetry: {season} {race}")
            try:
                agg = get_telemetry(season, race)
                season_agg.append(agg)
            except Exception as e:
                print(f"Error extracting {season} {race}: {e}")
                continue

        if season_agg:
            season_df = pd.concat(season_agg, ignore_index=True)
            all_agg.append(season_df)
            # Save incrementally after each season
            combined = pd.concat(all_agg, ignore_index=True)
            os.makedirs(RAW_DIR, exist_ok=True)
            combined.to_csv(f'{RAW_DIR}/telemetry_agg_2023_2025.csv', index=False)
            print(f"  Saved {len(combined)} agg rows after {season}")

    return pd.concat(all_agg, ignore_index=True)

def run():
    print("Extracting telemetry (agg only)")
    tel_agg = extract_all_races(YEARS)
    os.makedirs(RAW_DIR, exist_ok=True)
    tel_agg.to_csv(f'{RAW_DIR}/telemetry_agg_2023_2025.csv', index=False)
    print(f"Telemetry extraction complete")
    print(f"  Agg rows: {len(tel_agg)} → telemetry_agg_2023_2025.csv")

if __name__ == "__main__":
    run()