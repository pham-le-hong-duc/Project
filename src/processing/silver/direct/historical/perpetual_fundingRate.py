"""
Perpetual Funding Rate Historical Processing (Direct Copy - No Aggregation)
Copy funding rate data from S3 to TimescaleDB without aggregation
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from datetime import datetime, timezone, timedelta
from utils.s3_client import MinIOWriter
from utils.timescaledb_client import TimescaleDBClient


def process_perpetual_fundingrate(
    start_date: datetime = datetime(2025, 1, 1, tzinfo=timezone.utc),
    end_date: datetime = None
):
    """
    Process perpetual funding rate data from S3 to TimescaleDB.
    
    Args:
        start_date: Start date (default: 2025-01-01)
        end_date: End date (default: today)
    """
    if end_date is None:
        end_date = datetime.now(timezone.utc)
    
    print("="*60)
    print("PERPETUAL FUNDING RATE - HISTORICAL PROCESSING")
    print("="*60)
    print(f"Period: {start_date.date()} to {end_date.date()}")
    
    # Initialize clients
    s3_client = MinIOWriter(endpoint=None, access_key='admin',  # Will use env var MINIO_ENDPOINT 
                           secret_key='password', bucket='okx', secure=False)
    
    ts_client = TimescaleDBClient(host=None, port=None, database='okx',  # Will use env vars
                                 user='okx_user', password='okx_password')
    
    # Process each month (funding rate data is stored by month, not by day)
    total_records = 0
    processed_months = set()
    
    current_date = start_date
    while current_date <= end_date:
        # Get month string (YYYY-MM format)
        month_str = current_date.strftime('%Y-%m')
        
        # Skip if we already processed this month
        if month_str in processed_months:
            current_date += timedelta(days=1)
            continue
        
        processed_months.add(month_str)
        s3_path = f"perpetual_fundingRate/btc-usdt-swap/{month_str}.parquet"
        
        print(f"Processing {month_str}...", end=" ")
        
        try:
            # Read from S3
            df = s3_client.read_parquet(s3_path)
            
            if df is None or len(df) == 0:
                print("No data")
                current_date += timedelta(days=1)
                continue
            
            # Filter data to only include records within the date range
            # funding_time is in milliseconds timestamp
            start_ts = int(start_date.timestamp() * 1000)
            end_ts = int(end_date.timestamp() * 1000)
            
            # Filter by funding_time column
            df_filtered = df.filter(
                (df['funding_time'] >= start_ts) & 
                (df['funding_time'] <= end_ts)
            )
            
            if len(df_filtered) == 0:
                print("No data in date range")
                current_date += timedelta(days=1)
                continue
            
            # Upsert to TimescaleDB (no aggregation)
            rows = ts_client.upsert_dataframe(df_filtered, "perpetual_fundingrate", key_column="funding_time")
            total_records += rows
            print(f"Copied {rows} records (from {len(df)} total in month)")
            
        except Exception as e:
            print(f"Error: {e}")
        
        current_date += timedelta(days=1)
    
    # Close connection
    ts_client.close()
    
    print("\n" + "="*60)
    print(f"COMPLETED: {total_records} total records copied")
    print("="*60)


if __name__ == "__main__":
    process_perpetual_fundingrate()
