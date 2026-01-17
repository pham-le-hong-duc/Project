"""
DAG for historical data aggregation (Silver layer).
Backfills historical data from Bronze (MinIO/S3) to Silver (DuckDB).
Run ONCE after real-time consumers start, or manually when needed.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

# Import aggregate functions
from processing.silver.aggregate.historical.spot_trades import process_spot_trades
from processing.silver.aggregate.historical.perpetual_trades import process_perpetual_trades
from processing.silver.aggregate.historical.index_klines import process_index_klines
from processing.silver.aggregate.historical.perpetual_mark_klines import process_perpetual_mark_klines
from processing.silver.aggregate.historical.perpetual_orderbook import process_perpetual_orderbook
from processing.silver.direct.historical.perpetual_fundingRate import process_perpetual_fundingrate

# Default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}


def backfill_spot_trades():
    print("BACKFILLING: Spot Trades")
    try:
        process_spot_trades()
        print("✓ Spot trades backfill completed")
    except Exception as e:
        print(f"⚠️ WARNING: Spot trades backfill failed: {e}")


def backfill_perpetual_trades():
    print("BACKFILLING: Perpetual Trades")
    try:
        process_perpetual_trades()
        print("✓ Perpetual trades backfill completed")
    except Exception as e:
        print(f"⚠️ WARNING: Perpetual trades backfill failed: {e}")


def backfill_index_klines():
    print("BACKFILLING: Index Price Klines")
    try:
        process_index_klines()
        print("✓ Index klines backfill completed")
    except Exception as e:
        print(f"⚠️ WARNING: Index klines backfill failed: {e}")


def backfill_perpetual_mark_klines():
    print("BACKFILLING: Perpetual Mark Price Klines")
    try:
        process_perpetual_mark_klines()
        print("✓ Perpetual mark klines backfill completed")
    except Exception as e:
        print(f"⚠️ WARNING: Perpetual mark klines backfill failed: {e}")


def backfill_perpetual_orderbook():
    print("BACKFILLING: Perpetual OrderBook")
    try:
        process_perpetual_orderbook()
        print("✓ Perpetual orderbook backfill completed")
    except Exception as e:
        print(f"⚠️ WARNING: Perpetual orderbook backfill failed: {e}")


def backfill_perpetual_fundingrate():
    print("BACKFILLING: Perpetual Funding Rate")
    try:
        process_perpetual_fundingrate()
        print("✓ Funding rate backfill completed")
    except Exception as e:
        print(f"⚠️ WARNING: Funding rate backfill failed: {e}")


# Define DAG
with DAG(
    dag_id='02_aggregate_historical_data',
    default_args=default_args,
    description='Backfill historical data from Bronze to Silver (S3 → DuckDB)',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Don't backfill past runs
    max_active_runs=1,
) as dag:
    
    
    spot_trades_task = PythonOperator(
        task_id='backfill_spot_trades',
        python_callable=backfill_spot_trades,
    )
    
    perpetual_trades_task = PythonOperator(
        task_id='backfill_perpetual_trades',
        python_callable=backfill_perpetual_trades,
    )
    
    index_klines_task = PythonOperator(
        task_id='backfill_index_klines',
        python_callable=backfill_index_klines,
    )
    
    perpetual_mark_klines_task = PythonOperator(
        task_id='backfill_perpetual_mark_klines',
        python_callable=backfill_perpetual_mark_klines,
    )
    
    perpetual_orderbook_task = PythonOperator(
        task_id='backfill_perpetual_orderbook',
        python_callable=backfill_perpetual_orderbook,
    )
    
    perpetual_fundingrate_task = PythonOperator(
        task_id='backfill_perpetual_fundingrate',
        python_callable=backfill_perpetual_fundingrate,
    )
    
    # Tasks run in parallel (TimescaleDB supports concurrent writes)
    [
        spot_trades_task,
        perpetual_trades_task,
        index_klines_task,
        perpetual_mark_klines_task,
        perpetual_orderbook_task,
        perpetual_fundingrate_task,
    ]
