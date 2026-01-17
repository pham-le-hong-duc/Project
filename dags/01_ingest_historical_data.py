"""
Airflow DAG: Ingest Historical Data from OKX

This DAG downloads historical data from OKX REST API and stores it in MinIO.
Run this once to backfill historical data before starting real-time streaming.

Data types:
- Spot trades (Download + RestAPI)
- Perpetual trades (Download + RestAPI)
- Perpetual orderbook (Download only)
- Index price klines (RestAPI)
- Perpetual mark price klines (RestAPI)
- Perpetual funding rate (Download + RestAPI)

Schedule: Manual trigger (one-time backfill)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import asyncio

# Import download classes
from src.ingestion.spot_trades.Download import SpotTradesDownload
from src.ingestion.perpetual_trades.Download import PerpetualTradesDownload
from src.ingestion.perpetual_fundingRate.Download import PerpetualFundingRateDownload
from src.ingestion.perpetual_orderBook.Download import PerpetualOrderBookDownload

# Import RestAPI classes
from src.ingestion.spot_trades.RestAPI import SpotTradesRestAPI
from src.ingestion.perpetual_trades.RestAPI import PerpetualTradesRestAPI
from src.ingestion.perpetual_fundingRate.RestAPI import PerpetualFundingRateRestAPI
from src.ingestion.indexPriceKlines.RestAPI import IndexPriceKlinesRestAPI
from src.ingestion.perpetual_markPriceKlines.RestAPI import PerpetualMarkPriceKlinesRestAPI

# Import bronze processing classes
from src.processing.bronze.rearrange import MinIODataRearranger
from src.processing.bronze.dedupe import DuplicateChecker


# Configuration
SYMBOL_SPOT = "BTC-USDT"
SYMBOL_PERPETUAL = "BTC-USDT-SWAP"
BASE_START_DATE = "2025-01-01"  # Adjust this to your desired start date


# Task functions for Download
def spot_trades_download():
    """Download spot trades historical data"""
    downloader = SpotTradesDownload(
        symbol=SYMBOL_SPOT,
        base_start_date=BASE_START_DATE
    )
    downloader.run()


def perpetual_trades_download():
    """Download perpetual trades historical data"""
    downloader = PerpetualTradesDownload(
        symbol=SYMBOL_PERPETUAL,
        base_start_date=BASE_START_DATE
    )
    downloader.run()


def perpetual_funding_rate_download():
    """Download perpetual funding rate historical data"""
    downloader = PerpetualFundingRateDownload(
        symbol=SYMBOL_PERPETUAL,
        base_start_date=BASE_START_DATE
    )
    downloader.run()


def perpetual_orderbook_download():
    """Download perpetual orderbook historical data"""
    downloader = PerpetualOrderBookDownload(
        symbol=SYMBOL_PERPETUAL,
        base_start_date=BASE_START_DATE
    )
    downloader.run()


# Task functions for RestAPI (fill gaps)
def spot_trades_rest_api():
    """Fill gaps in spot trades using REST API"""
    api = SpotTradesRestAPI(
        symbol=SYMBOL_SPOT,
        base_start_date=BASE_START_DATE
    )
    asyncio.run(api.run())


def perpetual_trades_rest_api():
    """Fill gaps in perpetual trades using REST API"""
    api = PerpetualTradesRestAPI(
        symbol=SYMBOL_PERPETUAL,
        base_start_date=BASE_START_DATE
    )
    asyncio.run(api.run())


def perpetual_funding_rate_rest_api():
    """Fill gaps in perpetual funding rate using REST API"""
    api = PerpetualFundingRateRestAPI(
        symbol=SYMBOL_PERPETUAL,
        base_start_date=BASE_START_DATE
    )
    asyncio.run(api.run())


def index_klines_rest_api():
    """Fill gaps in index price klines using REST API"""
    api = IndexPriceKlinesRestAPI(
        symbol=SYMBOL_SPOT,
        base_start_date=BASE_START_DATE
    )
    asyncio.run(api.run())


def perpetual_mark_klines_rest_api():
    """Fill gaps in perpetual mark price klines using REST API"""
    api = PerpetualMarkPriceKlinesRestAPI(
        symbol=SYMBOL_PERPETUAL,
        base_start_date=BASE_START_DATE
    )
    asyncio.run(api.run())

# Rearrange functions
def rearrange_spot_trades():
    """Rearrange spot trades records."""
    rearranger = MinIODataRearranger()
    rearranger.rearrange_data_type('spot_trades')

def rearrange_perpetual_trades():
    """Rearrange perpetual trades records."""
    rearranger = MinIODataRearranger()
    rearranger.rearrange_data_type('perpetual_trades')

def rearrange_perpetual_orderbook():
    """Rearrange perpetual orderbook records."""
    rearranger = MinIODataRearranger()
    rearranger.rearrange_data_type('perpetual_orderBook')

def rearrange_perpetual_funding_rate():
    """Rearrange perpetual funding rate records."""
    rearranger = MinIODataRearranger()
    rearranger.rearrange_data_type('perpetual_fundingRate')

def rearrange_index_klines():
    """Rearrange index klines records."""
    rearranger = MinIODataRearranger()
    rearranger.rearrange_data_type('indexPriceKlines')

def rearrange_perpetual_mark_klines():
    """Rearrange perpetual mark klines records."""
    rearranger = MinIODataRearranger()
    rearranger.rearrange_data_type('perpetual_markPriceKlines')

# Dedupe functions
def dedupe_spot_trades():
    """Deduplicate spot trades."""
    checker = DuplicateChecker()
    checker.process_files_for_data_type('spot_trades', 'remove')

def dedupe_perpetual_trades():
    """Deduplicate perpetual trades."""
    checker = DuplicateChecker()
    checker.process_files_for_data_type('perpetual_trades', 'remove')

def dedupe_perpetual_orderbook():
    """Deduplicate perpetual orderbook."""
    checker = DuplicateChecker()
    checker.process_files_for_data_type('perpetual_orderBook', 'remove')

def dedupe_perpetual_funding_rate():
    """Deduplicate perpetual funding rate."""
    checker = DuplicateChecker()
    checker.process_files_for_data_type('perpetual_fundingRate', 'remove')

def dedupe_index_klines():
    """Deduplicate index klines."""
    checker = DuplicateChecker()
    checker.process_files_for_data_type('indexPriceKlines', 'remove')

def dedupe_perpetual_mark_klines():
    """Deduplicate perpetual mark klines."""
    checker = DuplicateChecker()
    checker.process_files_for_data_type('perpetual_markPriceKlines', 'remove')


# Define DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='01_ingest_historical_data',
    default_args=default_args,
    description='Download and fill historical data from OKX',
    schedule_interval=None,  
    start_date=datetime(2024, 1, 1),
    catchup=False,  
) as dag:
    
    # Task Group: Spot Trades (Ingestion + Bronze Processing)
    with TaskGroup('spot_trades', tooltip='Spot trades data pipeline') as spot_trades_group:
        task_download = PythonOperator(
            task_id='download',
            python_callable=spot_trades_download,
        )
        task_rest_api = PythonOperator(
            task_id='rest_api',
            python_callable=spot_trades_rest_api,
        )
        task_rearrange = PythonOperator(
            task_id='rearrange',
            python_callable=rearrange_spot_trades,
        )
        task_dedupe = PythonOperator(
            task_id='dedupe',
            python_callable=dedupe_spot_trades,
        )
        task_download >> task_rest_api >> task_rearrange >> task_dedupe
    
    # Task Group: Perpetual Trades (Ingestion + Bronze Processing)
    with TaskGroup('perpetual_trades', tooltip='Perpetual trades data pipeline') as perpetual_trades_group:
        task_download = PythonOperator(
            task_id='download',
            python_callable=perpetual_trades_download,
        )
        task_rest_api = PythonOperator(
            task_id='rest_api',
            python_callable=perpetual_trades_rest_api,
        )
        task_rearrange = PythonOperator(
            task_id='rearrange',
            python_callable=rearrange_perpetual_trades,
        )
        task_dedupe = PythonOperator(
            task_id='dedupe',
            python_callable=dedupe_perpetual_trades,
        )
        task_download >> task_rest_api >> task_rearrange >> task_dedupe
    
    # Task Group: Perpetual Funding Rate (Ingestion + Bronze Processing)
    with TaskGroup('perpetual_funding_rate', tooltip='Perpetual funding rate data pipeline') as perpetual_funding_rate_group:
        task_download = PythonOperator(
            task_id='download',
            python_callable=perpetual_funding_rate_download,
        )
        task_rest_api = PythonOperator(
            task_id='rest_api',
            python_callable=perpetual_funding_rate_rest_api,
        )
        task_rearrange = PythonOperator(
            task_id='rearrange',
            python_callable=rearrange_perpetual_funding_rate,
        )
        task_dedupe = PythonOperator(
            task_id='dedupe',
            python_callable=dedupe_perpetual_funding_rate,
        )
        task_download >> task_rest_api >> task_rearrange >> task_dedupe
    
    # Task Group: Perpetual OrderBook (Ingestion + Bronze Processing)
    with TaskGroup('perpetual_orderbook', tooltip='Perpetual OrderBook data pipeline') as perpetual_orderbook_group:
        task_download = PythonOperator(
            task_id='download',
            python_callable=perpetual_orderbook_download,
        )
        task_rearrange = PythonOperator(
            task_id='rearrange',
            python_callable=rearrange_perpetual_orderbook,
        )
        task_dedupe = PythonOperator(
            task_id='dedupe',
            python_callable=dedupe_perpetual_orderbook,
        )
        task_download >> task_rearrange >> task_dedupe
    
    # Task Group: Index Klines (Ingestion + Bronze Processing)
    with TaskGroup('index_klines', tooltip='Index klines data pipeline') as index_klines_group:
        task_rest_api = PythonOperator(
            task_id='rest_api',
            python_callable=index_klines_rest_api,
        )
        task_rearrange = PythonOperator(
            task_id='rearrange',
            python_callable=rearrange_index_klines,
        )
        task_dedupe = PythonOperator(
            task_id='dedupe',
            python_callable=dedupe_index_klines,
        )
        task_rest_api >> task_rearrange >> task_dedupe
    
    # Task Group: Perpetual Mark Klines (Ingestion + Bronze Processing)
    with TaskGroup('perpetual_mark_klines', tooltip='Perpetual mark price klines data pipeline') as perpetual_mark_klines_group:
        task_rest_api = PythonOperator(
            task_id='rest_api',
            python_callable=perpetual_mark_klines_rest_api,
        )
        task_rearrange = PythonOperator(
            task_id='rearrange',
            python_callable=rearrange_perpetual_mark_klines,
        )
        task_dedupe = PythonOperator(
            task_id='dedupe',
            python_callable=dedupe_perpetual_mark_klines,
        )
        task_rest_api >> task_rearrange >> task_dedupe
    
    # All task groups run independently (no dependencies between groups)
