import json
import pickle
import numpy as np
import snowflake.connector
from kafka import KafkaConsumer
from datetime import datetime
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

MODEL_FEATURES = [
    "tyre_life", "compound_encoded", "position", "lap_number", "stint",
    "track_temp_c", "air_temp_c", "is_raining", "avg_speed",
    "avg_throttle_pct", "heavy_braking_pct", "laps_remaining",
    "gap_to_car_ahead_seconds"
]

def load_model():
    with open("ml/pit_stop_model.pkl", "rb") as f:
        return pickle.load(f)

def get_snowflake_conn():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

def write_prediction(conn, msg, probability, recommendation):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO MART_PIT_PREDICTIONS_LIVE (
            session_key, driver_number, lap_number,
            lap_duration, pit_probability, pit_recommendation,
            predicted_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        msg["session_key"],
        msg["driver_number"],
        msg["lap_number"],
        msg["lap_duration"],
        float(probability),
        bool(recommendation),
        datetime.utcnow()
    ))
    cursor.close()

def main():
    model = load_model()
    conn = get_snowflake_conn()
    feature_store = build_feature_store(SESSION_KEY, DRIVERS)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
        auto_offset_reset="latest",
        group_id="pitwall-consumer"
    )

    print("Consumer started. Listening for messages...\n")

    for message in consumer:
        if message.value is None:
            continue
        msg = message.value
        try:
            key = (msg["driver_number"], msg["lap_number"])
            features = feature_store.get(key)

            if features is None:
                print(f"No features for driver {msg['driver_number']} lap {msg['lap_number']}, skipping")
                continue

            X = np.array([[features[f] for f in MODEL_FEATURES]])
            probability = model.predict_proba(X)[0][1]
            recommendation = probability >= PIT_THRESHOLD

            write_prediction(conn, msg, probability, recommendation)

            flag = "PIT NOW" if recommendation else "stay out"
            print(f"Driver {msg['driver_number']} Lap {msg['lap_number']} | "
                  f"prob={probability:.3f} | {flag}")

        except Exception as e:
            print(f"Error on driver {msg['driver_number']} lap {msg['lap_number']}: {e}")
            continue

if __name__ == "__main__":
    main()