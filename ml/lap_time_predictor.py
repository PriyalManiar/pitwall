import pandas as pd
import numpy as np
import pickle
import os
import getpass
import snowflake.connector

from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import cross_val_score, KFold
from sklearn.metrics import mean_squared_error, r2_score
import xgboost as xgb

MODEL_PATH = 'ml/lap_time_model.pkl'
ENCODER_PATH = 'ml/lap_time_compound_encoder.pkl'

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

def load_features(conn) -> pd.DataFrame:
    print("Loading mart_ml_features from Snowflake...")
    query = """
        SELECT
            lap_time_seconds,
            tyre_life,
            compound,
            stint,
            lap_number,
            position,
            track_temp_c,
            air_temp_c,
            is_raining,
            avg_speed,
            avg_throttle_pct,
            heavy_braking_pct
        FROM PITWALL.MARTS.MART_ML_FEATURES
        WHERE is_rep_lap = TRUE
          AND lap_time_seconds IS NOT NULL
          AND tyre_life IS NOT NULL
          AND avg_speed IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    print(f"Loaded {len(df)} rows")
    return df

def prepare_features(df: pd.DataFrame):
    le = LabelEncoder()
    df['compound_encoded'] = le.fit_transform(df['compound'].fillna('UNKNOWN'))

    feature_cols = [
        'tyre_life', 'compound_encoded', 'stint', 'lap_number',
        'position', 'track_temp_c', 'air_temp_c', 'is_raining',
        'avg_speed', 'avg_throttle_pct', 'heavy_braking_pct'
    ]

    X = df[feature_cols].fillna(0)
    y = df['lap_time_seconds']

    return X, y, le, feature_cols

def compare_models(X, y) -> dict:
    models = {
        'LinearRegression': LinearRegression(),
        'RandomForest': RandomForestRegressor(
            n_estimators=100, random_state=42, n_jobs=-1
        ),
        'XGBoost': xgb.XGBRegressor(
            n_estimators=100, random_state=42,
            verbosity=0
        )
    }

    cv = KFold(n_splits=5, shuffle=True, random_state=42)
    results = {}

    for name, model in models.items():
        print(f"Training {name}...")
        
        # RMSE via cross validation
        neg_mse = cross_val_score(model, X, y, cv=cv, scoring='neg_mean_squared_error', n_jobs=-1)
        rmse = np.sqrt(-neg_mse)
        
        # R² via cross validation
        r2 = cross_val_score(model, X, y, cv=cv, scoring='r2', n_jobs=-1)
        
        results[name] = {
            'model': model,
            'rmse_mean': rmse.mean(),
            'rmse_std': rmse.std(),
            'r2_mean': r2.mean(),
            'r2_std': r2.std()
        }
        print(f"  {name}: RMSE = {rmse.mean():.3f}s ± {rmse.std():.3f} | R² = {r2.mean():.4f} ± {r2.std():.4f}")

    return results

def write_predictions(conn, df, X, best_model):
    print("Writing lap time predictions to Snowflake...")

    predictions = best_model.predict(X)
    df['predicted_lap_time_seconds'] = predictions
    df['prediction_error_seconds'] = df['predicted_lap_time_seconds'] - df['lap_time_seconds']

    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS PITWALL.MARTS.MART_LAP_PREDICTIONS (
            lap_time_seconds        FLOAT,
            predicted_lap_time_seconds FLOAT,
            prediction_error_seconds FLOAT,
            tyre_life               FLOAT,
            compound                VARCHAR,
            lap_number              FLOAT,
            track_temp_c            FLOAT,
            is_raining              BOOLEAN,
            avg_speed               FLOAT
        )
    """)
    cursor.execute("TRUNCATE TABLE PITWALL.MARTS.MART_LAP_PREDICTIONS")

    rows = df[[
        'lap_time_seconds', 'predicted_lap_time_seconds', 'prediction_error_seconds',
        'tyre_life', 'compound', 'lap_number', 'track_temp_c', 'is_raining', 'avg_speed'
    ]].values.tolist()

    cursor.executemany(
        "INSERT INTO PITWALL.MARTS.MART_LAP_PREDICTIONS VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        rows
    )
    print(f"Written {len(rows)} predictions to Snowflake")
    cursor.close()

def run():
    conn = get_snowflake_connection()

    # Load data
    df = load_features(conn)
    X, y, le, feature_cols = prepare_features(df)

    print(f"\nTarget variable stats:")
    print(f"  Mean lap time: {y.mean():.3f}s")
    print(f"  Std lap time:  {y.std():.3f}s")
    print(f"  Min: {y.min():.3f}s | Max: {y.max():.3f}s")

    # Compare models
    print("\nComparing models...")
    results = compare_models(X, y)

    # Select best by RMSE
    best_name = min(results, key=lambda k: results[k]['rmse_mean'])
    best_model = results[best_name]['model']
    print(f"\nBest model: {best_name} (RMSE={results[best_name]['rmse_mean']:.3f}s, R²={results[best_name]['r2_mean']:.4f})")

    # Fit on full data
    print("Fitting best model on full dataset...")
    best_model.fit(X, y)

    # Save model
    os.makedirs('ml', exist_ok=True)
    with open(MODEL_PATH, 'wb') as f:
        pickle.dump(best_model, f)
    with open(ENCODER_PATH, 'wb') as f:
        pickle.dump(le, f)
    print(f"Model saved to {MODEL_PATH}")

    # Write predictions
    write_predictions(conn, df, X, best_model)

    conn.close()
    print("\nLap time predictor complete.")

if __name__ == "__main__":
    run()