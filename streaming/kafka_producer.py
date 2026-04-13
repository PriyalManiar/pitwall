import json
import time
import requests
from kafka import KafkaProducer

KAFKA_TOPIC = "pitwall.live.telemetry"
KAFKA_BROKER = "localhost:9092"

FIELDS = [
    "session_key", "driver_number", "lap_number", "lap_duration",
    "duration_sector_1", "duration_sector_2", "duration_sector_3",
    "i1_speed", "i2_speed", "st_speed", "is_pit_out_lap", "date_start"
]

SESSION_KEY = 9979
DRIVERS = [1, 4, 5, 6, 10, 12, 14, 16, 18, 22, 23, 27, 30, 31, 43, 44, 55, 63, 81, 87]

def fetch_laps(session_key, driver_number):
    url = f"https://api.openf1.org/v1/laps?session_key={session_key}&driver_number={driver_number}"
    response = requests.get(url)
    laps = response.json()
    if not laps or not isinstance(laps, list):
        print(f"  No data for driver {driver_number}, skipping")
        return []
    filtered = []
    for lap in laps:
        filtered.append({field: lap.get(field) for field in FIELDS})
    return sorted(filtered, key=lambda x: x["date_start"] or "")

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print(f"Fetching laps for session {SESSION_KEY}...")
    all_laps = []
    for driver in DRIVERS:
        laps = fetch_laps(SESSION_KEY, driver)
        all_laps.extend(laps)
        print(f"  Driver {driver}: {len(laps)} laps fetched")

    all_laps = sorted(all_laps, key=lambda x: x["date_start"] or "")
    print(f"\nTotal laps to stream: {len(all_laps)}")
    print("Starting replay...\n")

    for lap in all_laps:
        producer.send(KAFKA_TOPIC, value=lap)
        print(f"Published — Driver {lap['driver_number']} Lap {lap['lap_number']} | {lap['lap_duration']}s")
        time.sleep(4)

    producer.flush()
    print("Replay complete.")

if __name__ == "__main__":
    main()