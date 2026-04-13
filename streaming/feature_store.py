import requests
import numpy as np
from collections import defaultdict

def fetch_json(url):
    response = requests.get(url)
    data = response.json()
    if not isinstance(data, list):
        return []
    return data

def build_feature_store(session_key, drivers):
    print("Building feature store...")
    store = {}

    # fetch stints for all drivers
    print("  Fetching stints...")
    stints = {}
    for driver in drivers:
        data = fetch_json(f"https://api.openf1.org/v1/stints?session_key={session_key}&driver_number={driver}")
        stints[driver] = data

    # fetch position for all drivers
    print("  Fetching positions...")
    positions = {}
    for driver in drivers:
        data = fetch_json(f"https://api.openf1.org/v1/position?session_key={session_key}&driver_number={driver}")
        positions[driver] = sorted(data, key=lambda x: x["date"])

    # fetch weather (same for all drivers)
    print("  Fetching weather...")
    weather = fetch_json(f"https://api.openf1.org/v1/weather?session_key={session_key}")
    weather = sorted(weather, key=lambda x: x["date"])

    # fetch car data for all drivers
    print("  Fetching car data...")
    car_data = {}
    for driver in drivers:
        data = fetch_json(f"https://api.openf1.org/v1/car_data?session_key={session_key}&driver_number={driver}")
        car_data[driver] = sorted(data, key=lambda x: x["date"])

    # fetch laps to get date_start per lap
    print("  Fetching lap timestamps...")
    lap_times = {}
    for driver in drivers:
        data = fetch_json(f"https://api.openf1.org/v1/laps?session_key={session_key}&driver_number={driver}")
        for lap in data:
            lap_times[(driver, lap["lap_number"])] = {
                "date_start": lap.get("date_start"),
                "lap_duration": lap.get("lap_duration"),
                "total_laps": None
            }

    print(f"  Lap timestamps fetched: {len(lap_times)} entries")
    # compute total laps per driver
    for driver in drivers:
        driver_laps = [k[1] for k in lap_times if k[0] == driver]
        max_lap = max(driver_laps) if driver_laps else 0
        for lap in driver_laps:
            if (driver, lap) in lap_times:
                lap_times[(driver, lap)]["total_laps"] = max_lap

    print("  Computing features per driver per lap...")
    compound_map = {"SOFT": 0, "MEDIUM": 1, "HARD": 2, "INTERMEDIATE": 3, "WET": 4}

    for (driver, lap_number), lap_info in lap_times.items():
        date_start = lap_info["date_start"]
        if not date_start:
            continue

        # stint features
        stint_number = 1
        compound = "MEDIUM"
        tyre_life = 1
        for stint in stints.get(driver, []):
            if stint["lap_start"] <= lap_number <= stint["lap_end"]:
                stint_number = stint["stint_number"]
                compound = stint.get("compound", "MEDIUM")
                tyre_life = lap_number - stint["lap_start"] + 1
                break

        # position — most recent before lap start
        position = 10
        for pos in positions.get(driver, []):
            if pos["date"] <= date_start:
                position = pos["position"]
            else:
                break

        # weather — most recent before lap start
        track_temp = 40.0
        air_temp = 20.0
        is_raining = 0
        for w in weather:
            if w["date"] <= date_start:
                track_temp = w.get("track_temperature", 40.0)
                air_temp = w.get("air_temperature", 20.0)
                is_raining = int(w.get("rainfall", 0) > 0)
            else:
                break

        # car data — points within this lap window
        next_lap_key = (driver, lap_number + 1)
        date_end = lap_times.get(next_lap_key, {}).get("date_start", None)
        lap_car_data = [
            c for c in car_data.get(driver, [])
            if c["date"] >= date_start and (date_end is None or c["date"] < date_end)
        ]
        if lap_car_data:
            avg_speed = np.mean([c["speed"] for c in lap_car_data])
            avg_throttle = np.mean([c["throttle"] for c in lap_car_data])
            heavy_braking = np.mean([1 if c["brake"] > 0 else 0 for c in lap_car_data])
        else:
            avg_speed = 200.0
            avg_throttle = 50.0
            heavy_braking = 0.1

        # laps remaining
        total_laps = lap_info["total_laps"] or lap_number
        laps_remaining = max(0, total_laps - lap_number)

        store[(driver, lap_number)] = {
            "tyre_life": tyre_life,
            "compound_encoded": compound_map.get(compound, 1),
            "position": position,
            "lap_number": lap_number,
            "stint": stint_number,
            "track_temp_c": track_temp,
            "air_temp_c": air_temp,
            "is_raining": is_raining,
            "avg_speed": avg_speed,
            "avg_throttle_pct": avg_throttle,
            "heavy_braking_pct": heavy_braking,
            "laps_remaining": laps_remaining,
            "gap_to_car_ahead_seconds": 0.0
        }

    print(f"Feature store built: {len(store)} entries\n")
    return store