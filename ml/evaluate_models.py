import pandas as pd
import numpy as np
import pickle
import getpass
import snowflake.connector
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import (
    f1_score, precision_score, recall_score,
    mean_squared_error, r2_score
)

def get_snowflake_connection():
    user = input("Snowflake username: ")
    password = getpass.getpass("Snowflake password: ")
    return snowflake.connector.connect(
        user=user,
        password=password,
        account='AIZZHNC-TT89572',
        warehouse='PITWALL_WH',
        database='PITWALL',
        schema='MARTS'
    )

def load_pit_model():
    with open('ml/pit_stop_model.pkl', 'rb') as f:
        model = pickle.load(f)
    with open('ml/compound_encoder.pkl', 'rb') as f:
        le = pickle.load(f)
    return model, le

def load_lap_model():
    with open('ml/lap_time_model.pkl', 'rb') as f:
        model = pickle.load(f)
    with open('ml/lap_time_compound_encoder.pkl', 'rb') as f:
        le = pickle.load(f)
    return model, le

def evaluate_pit_model(conn, model, le):
    print("Evaluating pit stop model...")
    query = """
        SELECT
            tyre_life, compound, position, lap_number,
            stint, track_temp_c, air_temp_c, is_raining,
            avg_speed, avg_throttle_pct, heavy_braking_pct,
            pitted_this_lap
        FROM PITWALL.MARTS.MART_ML_FEATURES
        WHERE is_rep_lap = TRUE
          AND tyre_life IS NOT NULL
          AND position IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    df['compound_encoded'] = le.transform(df['compound'].fillna('UNKNOWN'))

    feature_cols = [
        'tyre_life', 'compound_encoded', 'position', 'lap_number',
        'stint', 'track_temp_c', 'air_temp_c', 'is_raining',
        'avg_speed', 'avg_throttle_pct', 'heavy_braking_pct'
    ]

    X = df[feature_cols].fillna(0)
    y = df['pitted_this_lap']

    y_pred = model.predict(X)
    y_proba = model.predict_proba(X)[:, 1]

    metrics = {
        'F1 Score':  f1_score(y, y_pred),
        'Precision': precision_score(y, y_pred),
        'Recall':    recall_score(y, y_pred),
        'Positive Rate (actual)':    y.mean(),
        'Positive Rate (predicted)': y_pred.mean()
    }

    # Feature importance if available
    importance = None
    if hasattr(model, 'feature_importances_'):
        importance = pd.Series(
            model.feature_importances_,
            index=feature_cols
        ).sort_values(ascending=False)

    return metrics, importance

def evaluate_lap_model(conn, model, le):
    print("Evaluating lap time model...")
    query = """
        SELECT
            lap_time_seconds, tyre_life, compound, stint,
            lap_number, position, track_temp_c, air_temp_c,
            is_raining, avg_speed, avg_throttle_pct, heavy_braking_pct
        FROM PITWALL.MARTS.MART_ML_FEATURES
        WHERE is_rep_lap = TRUE
          AND lap_time_seconds IS NOT NULL
          AND tyre_life IS NOT NULL
          AND avg_speed IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    df['compound_encoded'] = le.transform(df['compound'].fillna('UNKNOWN'))

    feature_cols = [
        'tyre_life', 'compound_encoded', 'stint', 'lap_number',
        'position', 'track_temp_c', 'air_temp_c', 'is_raining',
        'avg_speed', 'avg_throttle_pct', 'heavy_braking_pct'
    ]

    X = df[feature_cols].fillna(0)
    y = df['lap_time_seconds']

    y_pred = model.predict(X)

    metrics = {
        'RMSE (seconds)': np.sqrt(mean_squared_error(y, y_pred)),
        'R²':             r2_score(y, y_pred),
        'Mean actual (s)':    y.mean(),
        'Mean predicted (s)': y_pred.mean(),
        'Max error (s)':      abs(y - y_pred).max()
    }

    importance = None
    if hasattr(model, 'feature_importances_'):
        importance = pd.Series(
            model.feature_importances_,
            index=feature_cols
        ).sort_values(ascending=False)

    return metrics, importance

def run():
    conn = get_snowflake_connection()

    pit_model, pit_le = load_pit_model()
    lap_model, lap_le = load_lap_model()

    pit_metrics, pit_importance = evaluate_pit_model(conn, pit_model, pit_le)
    lap_metrics, lap_importance = evaluate_lap_model(conn, lap_model, lap_le)

    report = []
    report.append("=" * 60)
    report.append("PITWALL ML MODEL EVALUATION REPORT")
    report.append("=" * 60)

    report.append("\n--- PIT STOP PREDICTOR (Classification) ---")
    report.append(f"Model type: {type(pit_model).__name__}")
    for k, v in pit_metrics.items():
        report.append(f"  {k}: {v:.4f}")

    if pit_importance is not None:
        report.append("\nFeature Importance (Pit Stop):")
        for feat, imp in pit_importance.items():
            report.append(f"  {feat}: {imp:.4f}")

    report.append("\n--- LAP TIME PREDICTOR (Regression) ---")
    report.append(f"Model type: {type(lap_model).__name__}")
    for k, v in lap_metrics.items():
        report.append(f"  {k}: {v:.4f}")

    if lap_importance is not None:
        report.append("\nFeature Importance (Lap Time):")
        for feat, imp in lap_importance.items():
            report.append(f"  {feat}: {imp:.4f}")

    report.append("\n" + "=" * 60)

    report_str = "\n".join(report)
    print(report_str)

    with open('ml/model_evaluation_report.txt', 'w') as f:
        f.write(report_str)

    print("\nReport saved to ml/model_evaluation_report.txt")
    conn.close()

if __name__ == "__main__":
    run()