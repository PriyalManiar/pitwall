import pandas as pd
import numpy as np
import pickle
import os
import getpass
import snowflake.connector

from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import cross_val_score, StratifiedKFold
from sklearn.metrics import classification_report
import xgboost as xgb

MODEL_PATH = 'ml/pit_stop_model.pkl'
ENCODER_PATH = 'ml/compound_encoder.pkl'

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
            tyre_life,
            compound,
            position,
            lap_number,
            stint,
            track_temp_c,
            air_temp_c,
            is_raining,
            avg_speed,
            avg_throttle_pct,
            heavy_braking_pct,
            pitted_this_lap
        FROM PITWALL.MARTS.MART_ML_FEATURES
        WHERE is_rep_lap = TRUE
          AND tyre_life IS NOT NULL
          AND position IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    print(f"Loaded {len(df)} rows")
    return df

def prepare_features(df: pd.DataFrame):
    # Encode compound
    le = LabelEncoder()
    df['compound_encoded'] = le.fit_transform(df['compound'].fillna('UNKNOWN'))

    feature_cols = [
        'tyre_life', 'compound_encoded', 'position', 'lap_number',
        'stint', 'track_temp_c', 'air_temp_c', 'is_raining',
        'avg_speed', 'avg_throttle_pct', 'heavy_braking_pct'
    ]

    X = df[feature_cols].fillna(0)
    y = df['pitted_this_lap']

    return X, y, le, feature_cols

def compare_models(X, y) -> dict:
    models = {
        'LogisticRegression': LogisticRegression(
            class_weight='balanced', max_iter=1000, random_state=42
        ),
        'RandomForest': RandomForestClassifier(
            n_estimators=100, class_weight='balanced', random_state=42, n_jobs=-1
        ),
        'XGBoost': xgb.XGBClassifier(
            scale_pos_weight=(y == 0).sum() / (y == 1).sum(),
            random_state=42, eval_metric='logloss', verbosity=0
        )
    }

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    results = {}

    for name, model in models.items():
        print(f"Training {name}...")
        scores = cross_val_score(model, X, y, cv=cv, scoring='f1', n_jobs=-1)
        results[name] = {
            'model': model,
            'f1_mean': scores.mean(),
            'f1_std': scores.std()
        }
        print(f"  {name}: F1 = {scores.mean():.4f} ± {scores.std():.4f}")

    return results

def write_predictions(conn, df, X, best_model, feature_cols):
    print("Writing predictions to Snowflake...")
    
    proba = best_model.predict_proba(X)[:, 1]
    df['pit_probability'] = proba
    df['pit_predicted'] = (proba > 0.5).astype(int)

    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS PITWALL.MARTS.MART_PIT_PREDICTIONS (
            tyre_life FLOAT,
            compound VARCHAR,
            position FLOAT,
            lap_number FLOAT,
            stint FLOAT,
            pit_probability FLOAT,
            pit_predicted NUMBER,
            pitted_this_lap NUMBER
        )
    """)
    cursor.execute("TRUNCATE TABLE PITWALL.MARTS.MART_PIT_PREDICTIONS")

    rows = df[['tyre_life', 'compound', 'position', 'lap_number', 
               'stint', 'pit_probability', 'pit_predicted', 'pitted_this_lap']].values.tolist()
    
    cursor.executemany(
        "INSERT INTO PITWALL.MARTS.MART_PIT_PREDICTIONS VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
        rows
    )
    print(f"Written {len(rows)} predictions to Snowflake")
    cursor.close()

def run():
    conn = get_snowflake_connection()
    
    # Load data
    df = load_features(conn)
    X, y, le, feature_cols = prepare_features(df)
    
    print(f"\nClass distribution:")
    print(f"  No pit: {(y==0).sum()} ({(y==0).mean()*100:.1f}%)")
    print(f"  Pit:    {(y==1).sum()} ({(y==1).mean()*100:.1f}%)")
    
    # Compare models
    print("\nComparing models...")
    results = compare_models(X, y)
    
    # Select best
    best_name = max(results, key=lambda k: results[k]['f1_mean'])
    best_model = results[best_name]['model']
    print(f"\nBest model: {best_name} (F1={results[best_name]['f1_mean']:.4f})")
    
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
    write_predictions(conn, df, X, best_model, feature_cols)
    
    conn.close()
    print("\nPit stop predictor complete.")

if __name__ == "__main__":
    run()