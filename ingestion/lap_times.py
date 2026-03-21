import fastf1
import pandas as pd
import os

from ingestion.config import RACES_2024, YEAR, RAW_DIR

fastf1.Cache.enable_cache('cache')

def get_lap_times(season: int, race:str) -> pd.DataFrame:
    session = fastf1.get_session(season,race, 'R')
    session.load(telemetry= False, weather=False)

    laps = session.laps[[
        'Driver','DriverNumber','Team','LapNumber','LapTime',
        'Sector1Time','Sector2Time','Sector3Time',
        'Compound','Stint', 'TyreLife','FreshTyre','Position',
        'PitInTime','PitOutTime','IsAccurate','Deleted','TrackStatus'
    ]].copy()

    laps['Race'] = race
    laps['Year'] = season

    #Convert timedelta columns to float seconds for Snowflake
    for col in ['LapTime','Sector1Time','Sector2Time','Sector3Time', 'PitInTime','PitOutTime']:
        laps[col] = laps[col].dt.total_seconds()

    #Add is_rep_lap - source of truth for data quality
    #All models that need clean laps filter from this column
    laps['is_rep_lap'] = (
        (laps['IsAccurate']  == True) &
        (laps['Deleted'] == False) &
        (laps['LapNumber'] > 1) & #lap 1 excluded as different from other laps 
        (~laps['TrackStatus'].astype(str).str.contains('4|5')) & #exclude laps with 4: saftey car or 5: virtual safety car (VSC)
        (laps['PitInTime'].isna()) &
        (laps['PitOutTime'].isna())
    )

    # Separate TrackStatus for yellow flag cases
    laps['has_yellow_flag'] = (
        laps['TrackStatus'].astype(str).str.contains('2')
    )
    return laps

def extract_all_races(season: int = YEAR) -> pd.DataFrame:
    all_laps = []

    for race in RACES_2024:
        print(f"Extracting {race}")
        try:
            laps = get_lap_times(season, race)
            all_laps.append(laps)
        except Exception as e:
            print(f"Error extracting {race}: {e}")      
            continue
    return pd.concat(all_laps, ignore_index=True)
   
if __name__ == "__main__":
    print("Extracting lap times")
    df = extract_all_races(YEAR) 
    os.makedirs(RAW_DIR, exist_ok=True)  
    df.to_csv(f'{RAW_DIR}/lap_times_{YEAR}.csv', index=False)
    print(f"Lap times extraction complete for {len(df)} laps")