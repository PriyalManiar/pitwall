import pandas as pd
import numpy as np
import pickle
import os
import getpass
import warnings
warnings.filterwarnings('ignore')

import snowflake.connector
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score, StratifiedKFold
from sklearn.metrics import (
    f1_score, precision_score, recall_score,
    confusion_matrix, classification_report
)
from sklearn.preprocessing import LabelEncoder
import xgboost as xgb
import optuna
import shap
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

optuna.logging.set_verbosity(optuna.logging.WARNING)

MODEL_PATH = 'ml/pit_stop_model.pkl'
ENCODER_PATH = 'ml/compound_encoder.pkl'
PLOTS_DIR = 'ml/plots'

def get_snowflake_connection():
    print("\nSnowflake Connection")
    user = input("Username: ")
    password = getpass.getpass("Password: ")
    account = input("Account identifier : ")
    warehouse = input("Warehouse (default: PITWALL_WH): ") or 'PITWALL_WH'
    database = input("Database (default: PITWALL): ") or 'PITWALL'
    
    return snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema='MARTS'
    )

def load_features(conn) -> pd.DataFrame:
    print("Loading mart_ml_features from Snowflake...")
    query = """
        SELECT
            tyre_life, compound, position, lap_number,
            stint, track_temp_c, air_temp_c, is_raining,
            avg_speed, avg_throttle_pct, heavy_braking_pct,
            pitted_this_lap, year,
            laps_remaining, gap_to_car_ahead_seconds
        FROM PITWALL.MARTS.MART_ML_FEATURES
        WHERE tyre_life IS NOT NULL
          AND position IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    df.columns = df.columns.str.lower()
    print(f"Loaded {len(df)} rows")
    return df

def prepare_features(df: pd.DataFrame):
    le = LabelEncoder()
    df['compound_encoded'] = le.fit_transform(df['compound'].fillna('UNKNOWN'))

    feature_cols = [
        'tyre_life', 'compound_encoded', 'position', 'lap_number',
        'stint', 'track_temp_c', 'air_temp_c', 'is_raining',
        'avg_speed', 'avg_throttle_pct', 'heavy_braking_pct',
        'laps_remaining', 'gap_to_car_ahead_seconds'
    ]

    X = df[feature_cols].fillna(0)
    y = df['pitted_this_lap']

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
        'LogisticRegression': LogisticRegression(
            class_weight='balanced', max_iter=1000, random_state=42
        ),
        'RandomForest': RandomForestClassifier(
            n_estimators=100, class_weight='balanced',
            random_state=42, n_jobs=-1
        ),
        'XGBoost': xgb.XGBClassifier(
            scale_pos_weight=(y_train == 0).sum() / (y_train == 1).sum(),
            random_state=42, eval_metric='logloss', verbosity=0
        )
    }

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    results = {}

    for name, model in models.items():
        print(f"Training {name}...")
        scores = cross_val_score(model, X_train, y_train, cv=cv, scoring='f1', n_jobs=-1)
        results[name] = {
            'model': model,
            'f1_mean': scores.mean(),
            'f1_std': scores.std()
        }
        print(f"  {name}: F1 = {scores.mean():.4f} ± {scores.std():.4f}")

    return results

def tune_with_optuna(X_train, y_train, best_name) -> dict:
    print(f"\nTuning {best_name} with Optuna (50 trials)...")

    scale = (y_train == 0).sum() / (y_train == 1).sum()
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

    def objective(trial):
        if best_name == 'RandomForest':
            params = {
                'n_estimators': trial.suggest_int('n_estimators', 50, 300),
                'max_depth': trial.suggest_int('max_depth', 3, 8),
                'min_samples_split': trial.suggest_int('min_samples_split', 2, 20),
                'min_samples_leaf': trial.suggest_int('min_samples_leaf', 1, 10),
                'max_features': trial.suggest_float('max_features', 0.3, 0.8),
                'class_weight': 'balanced',
                'random_state': 42,
                'n_jobs': -1
            }
            model = RandomForestClassifier(**params)

        elif best_name == 'XGBoost':
            params = {
                'n_estimators': trial.suggest_int('n_estimators', 50, 300),
                'max_depth': trial.suggest_int('max_depth', 3, 8),
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
                'subsample': trial.suggest_float('subsample', 0.6, 1.0),
                'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
                'scale_pos_weight': scale,
                'random_state': 42,
                'eval_metric': 'logloss',
                'verbosity': 0
            }
            model = xgb.XGBClassifier(**params)

        else:
            params = {
                'C': trial.suggest_float('C', 0.01, 10.0),
                'class_weight': 'balanced',
                'max_iter': 1000,
                'random_state': 42
            }
            model = LogisticRegression(**params)

        scores = cross_val_score(model, X_train, y_train, cv=cv, scoring='f1', n_jobs=-1)
        return scores.mean()

    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=50)

    print(f"Best F1 after tuning: {study.best_value:.4f}")
    print(f"Best params: {study.best_params}")

    return study.best_params

def build_tuned_model(best_name, best_params, y_train):
    scale = (y_train == 0).sum() / (y_train == 1).sum()

    if best_name == 'RandomForest':
        return RandomForestClassifier(
            **best_params, class_weight='balanced',
            random_state=42, n_jobs=-1
        )
    elif best_name == 'XGBoost':
        return xgb.XGBClassifier(
            **best_params, scale_pos_weight=scale,
            random_state=42, eval_metric='logloss', verbosity=0
        )
    else:
        return LogisticRegression(
            **best_params, class_weight='balanced',
            max_iter=1000, random_state=42
        )

def plot_confusion_matrix(y_test, y_pred):
    os.makedirs(PLOTS_DIR, exist_ok=True)
    cm = confusion_matrix(y_test, y_pred)
    fig, ax = plt.subplots(figsize=(6, 5))
    im = ax.imshow(cm, cmap='Blues')
    plt.colorbar(im)
    ax.set_xticks([0, 1])
    ax.set_yticks([0, 1])
    ax.set_xticklabels(['No Pit', 'Pit'])
    ax.set_yticklabels(['No Pit', 'Pit'])
    ax.set_xlabel('Predicted')
    ax.set_ylabel('Actual')
    ax.set_title('Pit Stop Predictor — Confusion Matrix')
    for i in range(2):
        for j in range(2):
            ax.text(j, i, str(cm[i, j]), ha='center', va='center',
                   color='white' if cm[i, j] > cm.max()/2 else 'black')
    plt.tight_layout()
    plt.savefig(f'{PLOTS_DIR}/pit_stop_confusion_matrix.png', dpi=150)
    plt.close()
    print(f"Saved confusion matrix to {PLOTS_DIR}/pit_stop_confusion_matrix.png")

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
    plt.savefig(f'{PLOTS_DIR}/pit_stop_feature_importance.png', dpi=150)
    plt.close()
    print(f"Saved feature importance to {PLOTS_DIR}/pit_stop_feature_importance.png")

def plot_shap_values(model, X_test, feature_cols, model_name):
    os.makedirs(PLOTS_DIR, exist_ok=True)
    print("Computing SHAP values...")
    try:
        if 'XGBoost' in model_name or 'Random' in model_name:
            explainer = shap.TreeExplainer(model)
        else:
            explainer = shap.LinearExplainer(model, X_test)

        shap_values = explainer.shap_values(X_test.iloc[:500])

        if isinstance(shap_values, list):
            shap_values = shap_values[1]

        plt.figure(figsize=(10, 6))
        shap.summary_plot(
            shap_values,
            X_test.iloc[:500],
            feature_names=feature_cols,
            show=False
        )
        plt.title('SHAP Feature Impact — Pit Stop Predictor')
        plt.tight_layout()
        plt.savefig(f'{PLOTS_DIR}/pit_stop_shap.png', dpi=150, bbox_inches='tight')
        plt.close()
        print(f"Saved SHAP plot to {PLOTS_DIR}/pit_stop_shap.png")
    except Exception as e:
        print(f"SHAP failed: {e}")

def write_predictions(conn, df, X, best_model):
    print("Writing predictions to Snowflake...")
    proba = best_model.predict_proba(X)[:, 1]
    df = df.copy()
    df['pit_probability'] = proba
    df['pit_predicted'] = (proba > 0.5).astype(int)

    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS PITWALL.MARTS.MART_PIT_PREDICTIONS (
            tyre_life           FLOAT,
            compound            VARCHAR,
            position            FLOAT,
            lap_number          FLOAT,
            stint               FLOAT,
            year                NUMBER,
            pit_probability     FLOAT,
            pit_predicted       NUMBER,
            pitted_this_lap     NUMBER
        )
    """)
    cursor.execute("TRUNCATE TABLE PITWALL.MARTS.MART_PIT_PREDICTIONS")

    rows = df[[
        'tyre_life', 'compound', 'position', 'lap_number',
        'stint', 'year', 'pit_probability', 'pit_predicted', 'pitted_this_lap'
    ]].values.tolist()

    cursor.executemany(
        "INSERT INTO PITWALL.MARTS.MART_PIT_PREDICTIONS VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        rows
    )
    print(f"Written {len(rows)} predictions to Snowflake")
    cursor.close()

def run():
    os.makedirs(PLOTS_DIR, exist_ok=True)
    conn = get_snowflake_connection()

    df = load_features(conn)
    X, y, le, feature_cols = prepare_features(df)

    print(f"\nClass distribution:")
    print(f"  No pit: {(y==0).sum()} ({(y==0).mean()*100:.1f}%)")
    print(f"  Pit:    {(y==1).sum()} ({(y==1).mean()*100:.1f}%)")

    X_train, X_test, y_train, y_test = train_test_split_by_year(df, X, y)

    print("\nComparing models on training data...")
    results = compare_models(X_train, y_train)

    best_name = max(results, key=lambda k: results[k]['f1_mean'])
    print(f"\nBest model before tuning: {best_name} (F1={results[best_name]['f1_mean']:.4f})")

    best_params = tune_with_optuna(X_train, y_train, best_name)
    tuned_model = build_tuned_model(best_name, best_params, y_train)

    print("\nFitting tuned model on training data...")
    tuned_model.fit(X_train, y_train)

    y_pred = tuned_model.predict(X_test)
    print(f"\nTest set results (2025 season):")
    print(f"  F1:        {f1_score(y_test, y_pred):.4f}")
    print(f"  Precision: {precision_score(y_test, y_pred):.4f}")
    print(f"  Recall:    {recall_score(y_test, y_pred):.4f}")
    print(classification_report(y_test, y_pred, target_names=['No Pit', 'Pit']))

    print("\nThreshold analysis:")
    proba_test = tuned_model.predict_proba(X_test)[:, 1]
    for threshold in [0.1, 0.2, 0.3, 0.4, 0.5]:
        y_pred_t = (proba_test > threshold).astype(int)
        f1 = f1_score(y_test, y_pred_t)
        prec = precision_score(y_test, y_pred_t)
        rec = recall_score(y_test, y_pred_t)
        print(f"  threshold={threshold}: F1={f1:.3f} P={prec:.3f} R={rec:.3f}")

    plot_confusion_matrix(y_test, y_pred)
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
    print("\nPit stop predictor complete.")

if __name__ == "__main__":
    run()

   