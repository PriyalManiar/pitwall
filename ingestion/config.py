import os
import fastf1
import logging

#Suppress FastF1 info logs
logging.getLogger('fastf1').setLevel(logging.WARNING)

fastf1.Cache.enable_cache('cache')

#Season
YEAR = 2024

#Paths 
RAW_DIR = 'data/raw'

#2024 Race Calender
RACES_2024 = [
    'Bahrain Grand Prix',
    'Saudi Arabian Grand Prix',
    'Australian Grand Prix',
    'Japanese Grand Prix',
    'Chinese Grand Prix',
    'Miami Grand Prix',
    'Emilia Romagna Grand Prix',
    'Monaco Grand Prix',
    'Canadian Grand Prix',
    'Spanish Grand Prix',
    'Austrian Grand Prix',
    'British Grand Prix',
    'Hungarian Grand Prix',
    'Belgian Grand Prix',
    'Dutch Grand Prix',
    'Italian Grand Prix',
    'Azerbaijan Grand Prix',
    'Singapore Grand Prix',
    'United States Grand Prix',
    'Mexico City Grand Prix',
    'São Paulo Grand Prix',
    'Las Vegas Grand Prix',
    'Qatar Grand Prix',
    'Abu Dhabi Grand Prix',
]

#Season Types
RACE_SESSION = 'R'
QUALIFYING_SESSION = 'Q'