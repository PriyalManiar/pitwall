import fastf1
import pandas as pd
import os

fastf1.Cache.enable_cache('cache')

RACES_2024 = [
 'Bahrain', 'Sudi Arabia', 'Australia', 'Azerbaijan', 'Miami', 'Spain', 'Monaco', 'Azerbaijan', 'Canada', 'Austria',
 'United Kingdom', 'Hungary', 'Belgium', 'Netherlands', 'Italy', 'Singapore', 'Japan', 'Qatar', 'United States', 'Mexico', 'Brazil',
]