import pandas as pd
import numpy as np
import pickle
import os
import getpass
import warnings
warnings.filterwarnings('ignore')

import snowflake.connector
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import cross_val_score, KFold
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.preprocessing import LabelEncoder
import xgboost as xgb
import optuna
import shap
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

optuna.logging.set_verbosity(optuna.logging.WARNING)

MODEL_PATH = 'ml/lap_time_model.pkl'
ENCODER_PATH = 'ml/lap_time_compound_encoder.pkl'
PLOTS_DIR = 'ml/plots'

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
            lap_time_seconds, tyre_life, compound, stint,
            lap_number, position, track_temp_c, air_temp_c,
            is_raining, avg_speed, avg_throttle_pct,
            heavy_braking_pct, year,
            laps_remaining, gap_to_car_ahead_seconds,
            lap_time_delta_3_seconds
        FROM PITWALL.MARTS.MART_ML_FEATURES
        WHERE is_rep_lap = TRUE
          AND lap_time_seconds IS NOT NULL
          AND tyre_life IS NOT NULL
          AND avg_speed IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    df.columns = df.columns.str.lower()
    print(f"Loaded {len(df)} rows")
    return df

def prepare_features(df: pd.DataFrame):
    le = LabelEncoder()
    df['compound_encoded'] = le.fit_transform(df['compound'].fillna('UNKNOWN'))

    feature_cols = [
        'tyre_life', 'compound_encoded', 'stint', 'lap_number',
        'position', 'track_temp_c', 'air_temp_c', 'is_raining',
        'avg_speed', 'avg_throttle_pct', 'heavy_braking_pct',
        'laps_remaining', 'gap_to_car_ahead_seconds',
        'lap_time_delta_3_seconds'
    ]

    X = df[feature_cols].fillna(0)
    y = df['lap_time_seconds']

    return X, y, le, feature_cols

def train_test_split_by_year(df, X, y):
    train_mask = df['year'].isin([2023, 2024])
    test_mask = df['year'] == 2025

    X_train = X[train_mask]
    X_test = X[test_mask]
    y_train = y[train_mask]
    y_test = y[test_mask]

    print(f"Train: {len(X_train)} rows (2023-2024)")
    print(f"Test:  {len(X_test)} rows (2025)")

    return X_train, X_test, y_train, y_test

def compare_models(X_train, y_train) -> dict:
    models = {
        'LinearRegression': LinearRegression(),
        'RandomForest': RandomForestRegressor(
            n_estimators=100, random_state=42, n_jobs=-1
        ),
        'XGBoost': xgb.XGBRegressor(
            n_estimators=100, random_state=42, verbosity=0
        )
    }

    cv = KFold(n_splits=5, shuffle=True, random_state=42)
    results = {}

    for name, model in models.items():
        print(f"Training {name}...")
        neg_mse = cross_val_score(
            model, X_train, y_train,
            cv=cv, scoring='neg_mean_squared_error', n_jobs=-1
        )
        rmse = np.sqrt(-neg_mse)
        r2 = cross_val_score(
            model, X_train, y_train,
            cv=cv, scoring='r2', n_jobs=-1
        )
        results[name] = {
            'model': model,
            'rmse_mean': rmse.mean(),
            'rmse_std': rmse.std(),
            'r2_mean': r2.mean(),
            'r2_std': r2.std()
        }
        print(f"  {name}: RMSE={rmse.mean():.3f}s ± {rmse.std():.3f} | R²={r2.mean():.4f}")

    return results

def tune_with_optuna(X_train, y_train, best_name) -> dict:
    print(f"\nTuning {best_name} with Optuna (50 trials)...")
    cv = KFold(n_splits=5, shuffle=True, random_state=42)

    def objective(trial):
        if best_name == 'RandomForest':
            params = {
                'n_estimators': trial.suggest_int('n_estimators', 50, 300),
                'max_depth': trial.suggest_int('max_depth', 3, 15),
                'min_samples_split': trial.suggest_int('min_samples_split', 2, 20),
                'min_samples_leaf': trial.suggest_int('min_samples_leaf', 1, 10),
                'random_state': 42,
                'n_jobs': -1
            }
            model = RandomForestRegressor(**params)

        elif best_name == 'XGBoost':
            params = {
                'n_estimators': trial.suggest_int('n_estimators', 50, 300),
                'max_depth': trial.suggest_int('max_depth', 3, 10),
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
                'subsample': trial.suggest_float('subsample', 0.6, 1.0),
                'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
                'random_state': 42,
                'verbosity': 0
            }
            model = xgb.XGBRegressor(**params)

        else:
            scores = cross_val_score(
                LinearRegression(), X_train, y_train,
                cv=cv, scoring='neg_mean_squared_error', n_jobs=-1
            )
            return scores.mean()

        scores = cross_val_score(
            model, X_train, y_train,
            cv=cv, scoring='neg_mean_squared_error', n_jobs=-1
        )
        return scores.mean()

    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=50)

    print(f"Best RMSE after tuning: {np.sqrt(-study.best_value):.3f}s")
    print(f"Best params: {study.best_params}")

    return study.best_params

def build_tuned_model(best_name, best_params):
    if best_name == 'RandomForest':
        return RandomForestRegressor(
            **best_params, random_state=42, n_jobs=-1
        )
    elif best_name == 'XGBoost':
        return xgb.XGBRegressor(
            **best_params, random_state=42, verbosity=0
        )
    else:
        return LinearRegression()

def plot_residuals(y_test, y_pred):
    os.makedirs(PLOTS_DIR, exist_ok=True)
    residuals = y_test.values - y_pred
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    axes[0].scatter(y_pred, residuals, alpha=0.3, s=10, color='steelblue')
    axes[0].axhline(0, color='red', linestyle='--')
    axes[0].set_xlabel('Predicted Lap Time (s)')
    axes[0].set_ylabel('Residual (s)')
    axes[0].set_title('Residuals vs Predicted')

    axes[1].hist(residuals, bins=50, color='steelblue', edgecolor='white')
    axes[1].axvline(0, color='red', linestyle='--')
    axes[1].set_xlabel('Residual (s)')
    axes[1].set_ylabel('Count')
    axes[1].set_title('Residual Distribution')

    plt.suptitle('Lap Time Predictor — Residual Analysis')
    plt.tight_layout()
    plt.savefig(f'{PLOTS_DIR}/lap_time_residuals.png', dpi=150)
    plt.close()
    print(f"Saved residual plot to {PLOTS_DIR}/lap_time_residuals.png")

def plot_feature_importance(model, feature_cols, model_name):
    os.makedirs(PLOTS_DIR, exist_ok=True)
    if not hasattr(model, 'feature_importances_'):
        return
    importance = pd.Series(model.feature_importances_, index=feature_cols)
    importance = importance.sort_values(ascending=True)
    fig, ax = plt.subplots(figsize=(8, 6))
    importance.plot(kind='barh', ax=ax, color='steelblue')
    ax.set_title(f'Feature Importance — {model_name}')
    ax.set_xlabel('Importance')
    plt.tight_layout()
    plt.savefig(f'{PLOTS_DIR}/lap_time_feature_importance.png', dpi=150)
    plt.close()
    print(f"Saved feature importance to {PLOTS_DIR}/lap_time_feature_importance.png")

def plot_shap_values(model, X_test, feature_cols, model_name):
    os.makedirs(PLOTS_DIR, exist_ok=True)
    print("Computing SHAP values...")
    try:
        if 'XGBoost' in model_name or 'Random' in model_name:
            explainer = shap.TreeExplainer(model)
        else:
            explainer = shap.LinearExplainer(model, X_test)

        shap_values = explainer.shap_values(X_test.iloc[:500])

        plt.figure(figsize=(10, 6))
        shap.summary_plot(
            shap_values,
            X_test.iloc[:500],
            feature_names=feature_cols,
            show=False
        )
        plt.title('SHAP Feature Impact — Lap Time Predictor')
        plt.tight_layout()
        plt.savefig(
            f'{PLOTS_DIR}/lap_time_shap.png',
            dpi=150, bbox_inches='tight'
        )
        plt.close()
        print(f"Saved SHAP plot to {PLOTS_DIR}/lap_time_shap.png")
    except Exception as e:
        print(f"SHAP failed: {e}")

def write_predictions(conn, df, X, best_model):
    print("Writing lap time predictions to Snowflake...")
    predictions = best_model.predict(X)
    df = df.copy()
    df['predicted_lap_time_seconds'] = predictions
    df['prediction_error_seconds'] = df['predicted_lap_time_seconds'] - df['lap_time_seconds']

    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS PITWALL.MARTS.MART_LAP_PREDICTIONS (
            lap_time_seconds            FLOAT,
            predicted_lap_time_seconds  FLOAT,
            prediction_error_seconds    FLOAT,
            tyre_life                   FLOAT,
            compound                    VARCHAR,
            lap_number                  FLOAT,
            track_temp_c                FLOAT,
            is_raining                  BOOLEAN,
            avg_speed                   FLOAT,
            year                        NUMBER
        )
    """)
    cursor.execute("TRUNCATE TABLE PITWALL.MARTS.MART_LAP_PREDICTIONS")

    rows = df[[
        'lap_time_seconds', 'predicted_lap_time_seconds',
        'prediction_error_seconds', 'tyre_life', 'compound',
        'lap_number', 'track_temp_c', 'is_raining', 'avg_speed', 'year'
    ]].values.tolist()

    cursor.executemany(
        "INSERT INTO PITWALL.MARTS.MART_LAP_PREDICTIONS VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        rows
    )
    print(f"Written {len(rows)} predictions to Snowflake")
    cursor.close()

def run():
    os.makedirs(PLOTS_DIR, exist_ok=True)
    conn = get_snowflake_connection()

    df = load_features(conn)
    X, y, le, feature_cols = prepare_features(df)

    print(f"\nTarget variable stats:")
    print(f"  Mean lap time: {y.mean():.3f}s")
    print(f"  Std:           {y.std():.3f}s")
    print(f"  Min: {y.min():.3f}s | Max: {y.max():.3f}s")

    X_train, X_test, y_train, y_test = train_test_split_by_year(df, X, y)

    print("\nComparing models on training data...")
    results = compare_models(X_train, y_train)

    best_name = min(results, key=lambda k: results[k]['rmse_mean'])
    print(f"\nBest model before tuning: {best_name} (RMSE={results[best_name]['rmse_mean']:.3f}s)")

    best_params = tune_with_optuna(X_train, y_train, best_name)
    tuned_model = build_tuned_model(best_name, best_params)

    print("\nFitting tuned model on training data...")
    tuned_model.fit(X_train, y_train)

    y_pred = tuned_model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    print(f"\nTest set results (2025 season):")
    print(f"  RMSE: {rmse:.3f}s")
    print(f"  R²:   {r2:.4f}")
    print(f"  Mean error: {(y_pred - y_test.values).mean():.3f}s")
    print(f"  Max error:  {abs(y_pred - y_test.values).max():.3f}s")

    plot_residuals(y_test, y_pred)
    plot_feature_importance(tuned_model, feature_cols, best_name)
    plot_shap_values(tuned_model, X_test, feature_cols, best_name)

    print("\nFitting final model on full dataset...")
    tuned_model.fit(X, y)

    with open(MODEL_PATH, 'wb') as f:
        pickle.dump(tuned_model, f)
    with open(ENCODER_PATH, 'wb') as f:
        pickle.dump(le, f)
    print(f"Model saved to {MODEL_PATH}")

    write_predictions(conn, df, X, tuned_model)

    conn.close()
    print("\nLap time predictor complete.")

if __name__ == "__main__":
    run()