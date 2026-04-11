import fastf1
import logging
import os

logging.getLogger('fastf1').setLevel(logging.WARNING)
fastf1.Cache.enable_cache('cache')

YEARS = list(range(2023, 2026))  # 2023-2025
RAW_DIR = 'data/raw'
RACE_SESSION = 'R'
QUALIFYING_SESSION = 'Q'

def get_race_names(season: int) -> list:
    schedule = fastf1.get_event_schedule(season, include_testing=False)
    return schedule['EventName'].tolist()