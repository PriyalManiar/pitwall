import os
import fastf1

fastf1.Cache.enable_cache('cache')

#Season
YEAR = 2024

#Paths 
RAW_DIR = 'data/raw'

#2024 Race Calender
RACES_2024 = [
    'Bahrain', 'Saudi Arabia', 'Australia', 'Japan', 
    'China', 'Miami', 'Emilia Romagna', 'Monaco', 'Spain', 
    'Canada','Austria', 'Britain', 'Hungary', 'Belgium', 
    'Netherlands', 'Italy', 'Singapore', 'Azerbaijan', 'Mexico', 
    'United States','Brazil', 'Las Vegas', 'Abu Dhabi', 'Qatar']

#Season Types
RACE_SESSION = 'R'
QUALIFYING_SESSION = 'Q'