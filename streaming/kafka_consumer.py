import json
import pickle
import time
import pandas as pd
import numpy as np
import snowflake.connector
from kafka import KafkaConsumer
from datetime import datetime, timezone
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from feature_store import build_feature_store

KAFKA_TOPIC = "pitwall.live.telemetry"
KAFKA_BROKER = "localhost:9092"
PIT_THRESHOLD = 0.2
SESSION_KEY = 9979
DRIVERS = [1, 4, 5, 6, 10, 12, 14, 16, 18, 22, 23, 27, 30, 31, 43, 44, 55, 63, 81, 87]

SNOWFLAKE_CONFIG = {
    "account": "AIZZHNC-TT89572",
    "user": "PRIYALMANIAR",
    "password": input("Snowflake password: "),
    "warehouse": "PITWALL_WH",
    "database": "PITWALL",
    "schema": "MARTS"
}

PIT_FEATURES = [
    "tyre_life", "compound_encoded", "position", "lap_number", "stint",
    "track_temp_c", "air_temp_c", "is_raining", "avg_speed",
    "avg_throttle_pct", "heavy_braking_pct", "laps_remaining",
    "gap_to_car_ahead_seconds"
]

LAP_FEATURES = [
    "tyre_life", "compound_encoded", "stint", "lap_number", "position",
    "track_temp_c", "air_temp_c", "is_raining", "avg_speed",
    "avg_throttle_pct", "heavy_braking_pct", "laps_remaining",
    "gap_to_car_ahead_seconds"
]

def load_models():
    with open("ml/pit_stop_model.pkl", "rb") as f:
        pit_model = pickle.load(f)
    with open("ml/lap_time_model.pkl", "rb") as f:
        lap_model = pickle.load(f)
    return pit_model, lap_model

def get_snowflake_conn():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

def write_prediction(conn, msg, pit_probability, pit_recommendation, predicted_lap_time):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO MART_PIT_PREDICTIONS_LIVE (
            session_key, driver_number, lap_number,
            lap_duration, pit_probability, pit_recommendation,
            predicted_lap_time, predicted_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        msg["session_key"],
        msg["driver_number"],
        msg["lap_number"],
        msg["lap_duration"],
        float(pit_probability),
        bool(pit_recommendation),
        float(predicted_lap_time),
        datetime.now(timezone.utc)
    ))
    cursor.close()

def main():
    pit_model, lap_model = load_models()
    conn = get_snowflake_conn()
    feature_store = build_feature_store(SESSION_KEY, DRIVERS)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
        auto_offset_reset="earliest",
        group_id=f"pitwall-consumer-{int(time.time())}"
    )

    print("Consumer started. Listening for messages...\n")

    for message in consumer:
        if message.value is None:
            continue
        msg = message.value
        try:
            if msg["lap_number"] < 3:
                continue

            key = (msg["driver_number"], msg["lap_number"])
            features = feature_store.get(key)

            if features is None:
                print(f"No features for driver {msg['driver_number']} lap {msg['lap_number']}, skipping")
                continue

            X_pit = pd.DataFrame([{f: features[f] for f in PIT_FEATURES}])
            pit_probability = pit_model.predict_proba(X_pit)[0][1]
            pit_recommendation = pit_probability >= PIT_THRESHOLD

            X_lap = pd.DataFrame([{f: features[f] for f in LAP_FEATURES}])
            predicted_lap_time = lap_model.predict(X_lap)[0]

            write_prediction(conn, msg, pit_probability, pit_recommendation, predicted_lap_time)

            pit_flag = "PIT NOW" if pit_recommendation else "stay out"
            print(f"Driver {msg['driver_number']} Lap {msg['lap_number']} | "
                  f"pit={pit_probability:.3f} {pit_flag} | "
                  f"next lap: {predicted_lap_time:.1f}s")

        except Exception as e:
            print(f"Error on driver {msg['driver_number']} lap {msg['lap_number']}: {e}")
            continue

if __name__ == "__main__":
    main()