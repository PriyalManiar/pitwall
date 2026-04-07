import fastf1
import pandas as pd
import os

from ingestion.config import RACES_2024, YEAR, RAW_DIR, RACE_SESSION


def get_results(season: int, race: str) -> pd.DataFrame:
    """
    Extract final race classification per driver per race.
    Uses ClassifiedPosition for points — official FIA result after penalties.
    Uses Position for race narrative — physical road finish order.
    See DECISIONS.md #006 for position field rationale.
    See DECISIONS.md #010 for DNF classification logic.
    """
    session = fastf1.get_session(season, race, RACE_SESSION)
    session.load(telemetry=False, weather=False)

    results = session.results[[
        'DriverNumber', 'Abbreviation', 'FullName',
        'TeamName', 'GridPosition',
        'Position', 'ClassifiedPosition',
        'Points', 'Status', 'Time'
    ]].copy()

    results['Race'] = race
    results['Year'] = season

    # Convert race time timedelta to seconds for Snowflake
    results['Time'] = results['Time'].dt.total_seconds()

    # Convert positions to numeric FIRST before any arithmetic
    # ClassifiedPosition can contain 'NC' (not classified) → becomes NaN
    results['ClassifiedPosition'] = pd.to_numeric(
        results['ClassifiedPosition'], errors='coerce'
    )
    results['GridPosition'] = pd.to_numeric(
        results['GridPosition'], errors='coerce'
    )

    # Positions gained — how many places gained or lost during race
    # Positive = gained places, Negative = lost places
    results['positions_gained'] = (
        results['GridPosition'] - results['ClassifiedPosition']
    )

    # is_dnf — FastF1 uses 'Retired' and 'Did not start' for non-finishers
    # 'Finished', 'Lapped', 'Disqualified' all completed race distance
    # See DECISIONS.md #010 for full status value breakdown
    results['is_dnf'] = results['Status'].isin(['Retired', 'Did not start'])

    # DNF type — FastF1 results don't distinguish mechanical vs driver error
    # Detailed retirement reasons come from Ergast — joined in dbt
    # All retirements marked mechanical here, refined in int_driver_era_unified
    results['dnf_type'] = 'none'
    results.loc[results['is_dnf'], 'dnf_type'] = 'mechanical'

    return results


def extract_all_races(season: int = YEAR) -> pd.DataFrame:
    all_results = []

    for race in RACES_2024:
        print(f"  Extracting results: {race}...")
        try:
            df = get_results(season, race)
            all_results.append(df)
        except Exception as e:
            print(f" Failed {race}: {type(e).__name__}: {e}")
            continue

    if not all_results:
        raise RuntimeError("No races loaded — check errors above")

    return pd.concat(all_results, ignore_index=True)


def run():
    print("Starting results extraction...")
    df = extract_all_races(YEAR)
    os.makedirs(RAW_DIR, exist_ok=True)
    df.to_csv(f'{RAW_DIR}/results_{YEAR}.csv', index=False)
    print(f"\nResults extraction complete — {len(df)} records across {df['Race'].nunique()} races")
    print(f"   DNFs: {df['is_dnf'].sum()}")
    print(f"   Mechanical DNFs: {(df['dnf_type'] == 'mechanical').sum()}")
    print(f"   Driver DNFs: {(df['dnf_type'] == 'driver').sum()}")
    print(f"   Avg positions gained: {df['positions_gained'].mean():.1f}")

if __name__ == "__main__":
    run()