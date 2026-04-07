import fastf1
import pandas as pd
import os

from ingestion.config import RACES_2024, YEAR, RAW_DIR, RACE_SESSION


def get_telemetry(season: int, race: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract telemetry data for all laps all drivers for a given race.
    Returns two DataFrames:
    - tel_raw: filtered columns for ML feature engineering
    - tel_agg: aggregated per lap for dashboard queries
    See DECISIONS.md #003 for storage strategy rationale.
    """
    session = fastf1.get_session(season, race, RACE_SESSION)
    session.load(telemetry=True, weather=False)

    tel_raw_all = []
    tel_agg_all = []

    for driver in session.drivers:
        driver_laps = session.laps.pick_drivers(driver)

        for _, lap in driver_laps.iterlaps():
            try:
                tel = lap.get_telemetry()

                if tel is None or len(tel) == 0:
                    continue

                # ── Raw store — filtered columns, car data only ────────────────
                # Interpolated points excluded — Source='car' only
                # See DECISIONS.md #003
                tel_car = tel[tel['Source'] == 'car'][[
                    'Distance', 'Speed', 'Throttle', 'Brake', 'X', 'Y'
                ]].copy()

                tel_car['Driver'] = lap['Driver']
                tel_car['LapNumber'] = lap['LapNumber']
                tel_car['Race'] = race
                tel_car['Year'] = season

                tel_raw_all.append(tel_car)

                # ── Aggregated store — one row per lap for dashboards ──────────
                tel_agg_all.append({
                    'Driver':            lap['Driver'],
                    'LapNumber':         lap['LapNumber'],
                    'Race':              race,
                    'Year':              season,
                    'max_speed':         tel['Speed'].max(),
                    'min_speed':         tel['Speed'].min(),
                    'avg_speed':         tel['Speed'].mean(),
                    'full_throttle_pct': (tel['Throttle'] == 100).mean(),
                    'heavy_braking_pct': tel['Brake'].mean(),
                    'drs_open_pct':      (tel['DRS'] >= 10).mean(),
                })

            except Exception:
                # Some laps have no telemetry — skip silently
                continue

    tel_raw = pd.concat(tel_raw_all, ignore_index=True) if tel_raw_all else pd.DataFrame()
    tel_agg = pd.DataFrame(tel_agg_all)

    return tel_raw, tel_agg


def extract_all_races(season: int = YEAR) -> tuple[pd.DataFrame, pd.DataFrame]:
    all_raw = []
    all_agg = []

    for race in RACES_2024:
        print(f"  Extracting telemetry: {race}")
        try:
            raw, agg = get_telemetry(season, race)
            if len(raw) > 0:
                all_raw.append(raw)
            if len(agg) > 0:
                all_agg.append(agg)
            print(f"  Raw rows: {len(raw):,} | Agg rows: {len(agg)}")
        except Exception as e:
            print(f"  Failed {race}: {type(e).__name__}: {e}")
            continue

    tel_raw = pd.concat(all_raw, ignore_index=True) if all_raw else pd.DataFrame()
    tel_agg = pd.concat(all_agg, ignore_index=True) if all_agg else pd.DataFrame()

    return tel_raw, tel_agg


def run():
    print("Starting telemetry extraction...")
    tel_raw, tel_agg = extract_all_races(YEAR)

    os.makedirs(RAW_DIR, exist_ok=True)

    tel_raw.to_csv(f'{RAW_DIR}/telemetry_raw_{YEAR}.csv', index=False)
    tel_agg.to_csv(f'{RAW_DIR}/telemetry_agg_{YEAR}.csv', index=False)

    print(f"\nTelemetry extraction complete")
    print(f"   Raw rows: {len(tel_raw):,} → telemetry_raw_{YEAR}.csv")
    print(f"   Agg rows: {len(tel_agg):,} → telemetry_agg_{YEAR}.csv")
    print(f"   Races: {tel_agg['Race'].nunique()}")

if __name__ == "__main__":
    run()