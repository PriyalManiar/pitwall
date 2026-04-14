# PitWall — F1 Race Intelligence Pipeline

> An end-to-end data engineering and machine learning portfolio project that ingests, transforms, models, and streams Formula 1 race data — from 1950 historical results to live 2026 race inference.

**Dashboard:** *(Tableau Public link — coming soon)*

---

PitWall is a production-realistic F1 race intelligence pipeline built to answer two questions during a live race:

1. **Should this driver pit now?** — XGBoost classifier scoring every lap in real time
2. **How fast will their next lap be?** — RandomForest regressor predicting lap time from telemetry and strategy context

The project spans the full data engineering stack: batch ingestion, cloud warehousing, dbt transformations, ML training, Kafka streaming, and a Tableau dashboard — all wired together with Airflow orchestration.

---

## Architecture

<img width="1020" height="459" alt="Data Pipeline Architecture" src="https://github.com/user-attachments/assets/0e0c1255-0b59-4b95-9aa0-68cb236423f8" />

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, FastF1, OpenF1 API, f1db |
| Storage | Snowflake (RAW + MARTS schemas) |
| Transformation | dbt (staging, intermediate, marts) |
| Orchestration | Airflow (Docker, CeleryExecutor) |
| Streaming | Kafka (KRaft, Docker), kafka-python |
| ML | XGBoost, scikit-learn, SHAP, Optuna |
| Visualization | Tableau Public |
| Infrastructure | Docker, Docker Compose |
| Language | Python 3.14 |

---

## Data Coverage

| Source | Coverage | Rows | Purpose |
|---|---|---|---|
| FastF1 | 2023–2025 | 77,720 laps | ML training, strategy analysis |
| Telemetry (raw) | 2023–2025 | 27,203,921 | Feature engineering |
| f1db | 1950–2022 | 25,892 results | Historical champions dashboard |
| OpenF1 (Kafka) | 2026 live / 2025 replay | streaming | Live race inference |

---

## ML Models

### Pit Stop Predictor
- **Model:** XGBoost classifier
- **Target:** Will this driver pit in the next lap?
- **Train:** 2023–2024 seasons | **Test:** 2025 season (year-based split prevents leakage)
- **Results:** F1=0.222, Precision=0.156, Recall=0.383
- **Live threshold:** 0.2 (tuned for recall — missing a pit window costs positions)
- **Key features:** tyre life, gap to car ahead, position, compound, track temp

### Lap Time Predictor
- **Model:** RandomForest regressor
- **Target:** Predicted lap time in seconds
- **Train:** 2023–2024 seasons | **Test:** 2025 season
- **Results:** RMSE=2.84s, R²=0.927, 70.7% within 2s, 93.8% within 5s
- **Live use:** Displayed per driver per lap in Tableau dashboard

Both models use SHAP for explainability. Hyperparameters tuned via Optuna (50 trials, Bayesian optimization).

---

## Kafka Streaming Pipeline
OpenF1 API
→ kafka_producer.py (replay: Monaco 2025 / live: current session)
→ Kafka topic: pitwall.live.telemetry (KRaft, single broker, Docker)
→ kafka_consumer.py
- feature_store.py builds enrichment lookup at startup
(stints, position, intervals, weather, car data from OpenF1)
- XGBoost scores pit probability at threshold=0.2
- RandomForest predicts next lap time
- Writes to MART_PIT_PREDICTIONS_LIVE in Snowflake
→ Tableau refreshes live tiles

**Feature store pattern:** All enrichment data pre-fetched once at startup and cached in memory. Instant O(1) lookup per message — no per-message API calls, no rate limit risk during a race.

---

## dbt Models

### Staging (6 models — views)
Clean and rename raw tables. Add surrogate keys via `dbt_utils.generate_surrogate_key()`. Add `is_rep_lap` flag excluding SC laps, VSC laps, pit laps, and lap 1.

### Intermediate (4 models — views)
- `int_driver_race_performance` — joins results, laps, pit stops
- `int_pit_strategy_outcomes` — self-join to derive undercut/overcut outcomes
- `int_weather_pace_impact` — window functions for pace delta vs dry baseline
- `int_driver_era_unified` — unifies FastF1 (2023–2025) and f1db (1950–2022)

### Marts (6 models — tables)
- `mart_driver_season_performance`
- `mart_constructor_standings`
- `mart_tire_degradation`
- `mart_pit_strategy`
- `mart_historical_champions` (1950–2025, 73 seasons)
- `mart_ml_features` (77,720 rows, all engineered features)

---

## Project Structure

## Project Structure

```
pitwall/
├── ingestion/
│   ├── config.py
│   ├── lap_times.py
│   ├── weather.py
│   ├── pit_stops.py
│   ├── results.py
│   ├── telemetry.py
│   ├── f1db.py
│   └── load_telemetry_raw.py
├── ml/
│   ├── pit_stop_predictor.py
│   ├── lap_time_predictor.py
│   └── plots/
├── streaming/
│   ├── kafka_producer.py
│   ├── kafka_consumer.py
│   └── feature_store.py
├── dags/
│   └── pitwall_dag.py
├── pitwall_dbt/
│   └── models/
│       ├── staging/
│       ├── intermediate/
│       └── marts/
├── assets/
├── Dockerfile
└── docker-compose.yaml
```
---

## Key Engineering Decisions

A full decision log with 34+ entries is documented in `DECISIONS.md`. Highlights:

- **Year-based train/test split** — prevents data leakage from same-race laps appearing in both sets
- **KRaft mode** — Kafka without Zookeeper, one fewer service to manage
- **Feature store pattern** — pre-fetch enrichment once at startup, O(1) in-memory lookup per message
- **Two-table telemetry strategy** — raw 27M rows for ML, aggregated for dashboards
- **AsOf join for weather** — handles variable lap lengths, no bucket misalignment
- **Threshold=0.2 for live inference** — asymmetric cost: missing a pit window > false alarm

---

## Running the Project

### Prerequisites
- Python 3.11+ (dbt venv) and Python 3.14 (main venv)
- Docker Desktop
- Snowflake account
- FastF1 cache directory

### Batch pipeline
```bash
# 1. Activate venv and run ingestion
source venv/bin/activate
python ingestion/lap_times.py
python ingestion/weather.py
python ingestion/pit_stops.py
python ingestion/results.py
python ingestion/telemetry.py
python ingestion/f1db.py

# 2. Run dbt transformations
source venv_dbt/bin/activate
cd pitwall_dbt && dbt run && dbt test

# 3. Train ML models
source venv/bin/activate
python ml/pit_stop_predictor.py
python ml/lap_time_predictor.py
```

### Streaming pipeline (live race / replay)
```bash
# 1. Start Kafka
docker compose up -d kafka

# 2. Start consumer (Terminal 1)
python streaming/kafka_consumer.py

# 3. Start producer (Terminal 2)
python streaming/kafka_producer.py
```

### Airflow
```bash
docker compose up -d
# UI at localhost:8080, admin/admin
```
<img width="1424" height="768" alt="Airflow UI" src="https://github.com/user-attachments/assets/7db3eb01-6b43-4652-aa44-9d410ad55893" />

---

## Results at a Glance

| Metric | Value |
|---|---|
| Total laps ingested | 77,720 |
| Raw telemetry rows | 27,203,921 |
| Historical results (f1db) | 25,892 |
| dbt models | 15 (6 staging, 4 intermediate, 6 marts) |
| dbt tests passing | all |
| Pit stop model F1 | 0.222 |
| Lap time model R² | 0.927 |
| Live predictions (Monaco 2025 replay) | 1,425 laps, 20 drivers |

## Dashboard


*Live pit probability, predicted lap time, tire degradation, and historical champion analysis — built in Tableau Public.*

**[View live dashboard →](YOUR_TABLEAU_PUBLIC_LINK)**
---
