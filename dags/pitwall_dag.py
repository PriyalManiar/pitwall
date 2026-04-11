from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from ingestion import lap_times, weather, pit_stops, results, telemetry, f1db

with DAG(
    dag_id='pitwall_ingestion',
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only — no automatic scheduling
    catchup=False,            # Don't backfill missed runs
    tags=['pitwall'],
) as dag:

    start = EmptyOperator(task_id='start')

    t_f1db = PythonOperator(
        task_id='ingest_f1db',
        python_callable=f1db.run,
    )
    
    t_lap_times = PythonOperator(
        task_id='ingest_lap_times',
        python_callable=lap_times.run,
    )

    t_weather = PythonOperator(
        task_id='ingest_weather',
        python_callable=weather.run,
    )

    t_pit_stops = PythonOperator(
        task_id='ingest_pit_stops',
        python_callable=pit_stops.run,
    )

    t_results = PythonOperator(
        task_id='ingest_results',
        python_callable=results.run,
    )

    t_telemetry = PythonOperator(
        task_id='ingest_telemetry',
        python_callable=telemetry.run,
    )

    done = EmptyOperator(task_id='done')

    start >> t_f1db >> t_lap_times >> t_weather >> t_pit_stops >> t_results >> t_telemetry >> done