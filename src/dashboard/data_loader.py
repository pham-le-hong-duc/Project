"""
Data Loader for TimescaleDB
Load OHLC data from index price klines tables
"""
import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from typing import Optional, List


class DataLoader:
    """Load data from TimescaleDB for dashboard visualization"""
    
    # Available intervals and their corresponding table names
    INTERVALS = {
        '5m': 'indexpriceklines_5m',
        '15m': 'indexpriceklines_15m',
        '1h': 'indexpriceklines_1h',
        '4h': 'indexpriceklines_4h',
        '1d': 'indexpriceklines_1d'
    }
    
    # Interval to milliseconds mapping (for filtering correct step size)
    INTERVAL_TO_MS = {
        '5m': 5 * 60 * 1000,
        '15m': 15 * 60 * 1000,
        '1h': 60 * 60 * 1000,
        '4h': 4 * 60 * 60 * 1000,
        '1d': 24 * 60 * 60 * 1000
    }
    
    # Interval to timedelta mapping (for calculating window start time)
    INTERVAL_TO_TIMEDELTA = {
        '5m': timedelta(minutes=5),
        '15m': timedelta(minutes=15),
        '1h': timedelta(hours=1),
        '4h': timedelta(hours=4),
        '1d': timedelta(days=1)
    }
    
    def __init__(self):
        """Initialize connection to TimescaleDB using Streamlit secrets"""
        self.config = st.secrets["timescaledb"]
        self.conn = None
        self._connect()
    
    def _connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(
                host=self.config["host"],
                port=self.config["port"],
                database=self.config["database"],
                user=self.config["user"],
                password=self.config["password"]
            )
        except Exception as e:
            st.error(f"Failed to connect to TimescaleDB: {e}")
            raise
    
    def load_ohlc(self, 
                  interval: str = '1h',
                  start_date: Optional[datetime] = None,
                  end_date: Optional[datetime] = None,
                  limit: Optional[int] = None) -> pd.DataFrame:
        """
        Load OHLC data from index price klines table
        
        Args:
            interval: Time interval ('5m', '15m', '1h', '4h', '1d')
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: now)
            limit: Maximum number of records to return
            
        Returns:
            DataFrame with columns: time, timestamp_dt, open, high, low, close
            - time: window start time (timestamp_dt - interval)
            - timestamp_dt: window end time
        """
        # Validate interval
        if interval not in self.INTERVALS:
            raise ValueError(f"Invalid interval. Must be one of: {list(self.INTERVALS.keys())}")
        
        table_name = self.INTERVALS[interval]
        interval_ms = self.INTERVAL_TO_MS[interval]
        
        # Set default dates
        if end_date is None:
            end_date = datetime.now()
        if start_date is None:
            start_date = end_date - timedelta(days=7)
        
        # Build query with filtering by step size
        # Only select records where ts_ms is aligned to the interval step
        # Use tuple for params instead of list for psycopg2 compatibility
        start_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
        
        query = f"""
            SELECT 
                timestamp_dt,
                ts_ms,
                open,
                high,
                low,
                close
            FROM {table_name}
            WHERE timestamp_dt >= '{start_str}'
              AND timestamp_dt <= '{end_str}'
              AND ts_ms % {interval_ms} = 0
            ORDER BY ts_ms ASC
        """
        
        # Add limit if specified
        if limit:
            query += f" LIMIT {limit}"
        
        # Execute query
        try:
            df = pd.read_sql(query, self.conn)
            
            # Convert timestamp_dt to datetime if not already
            if not pd.api.types.is_datetime64_any_dtype(df['timestamp_dt']):
                df['timestamp_dt'] = pd.to_datetime(df['timestamp_dt'])
            
            # Calculate window start time: time = timestamp_dt - interval
            interval_delta = self.INTERVAL_TO_TIMEDELTA[interval]
            df['time'] = df['timestamp_dt'] - interval_delta
            
            # Reorder columns: time first, then timestamp_dt, then OHLC
            df = df[['time', 'timestamp_dt', 'ts_ms', 'open', 'high', 'low', 'close']]
            
            return df
        
        except Exception as e:
            st.error(f"Failed to load data: {e}")
            raise
    
    def get_available_date_range(self, interval: str = '1h') -> tuple:
        """
        Get the available date range for a given interval
        
        Args:
            interval: Time interval
            
        Returns:
            Tuple of (min_date, max_date)
        """
        if interval not in self.INTERVALS:
            raise ValueError(f"Invalid interval. Must be one of: {list(self.INTERVALS.keys())}")
        
        table_name = self.INTERVALS[interval]
        
        query = f"""
            SELECT 
                MIN(timestamp_dt) as min_date,
                MAX(timestamp_dt) as max_date
            FROM {table_name}
        """
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            
            if result and result[0] and result[1]:
                min_date = pd.to_datetime(result[0])
                max_date = pd.to_datetime(result[1])
                return (min_date, max_date)
            else:
                return (None, None)
        
        except Exception as e:
            st.error(f"Failed to get date range: {e}")
            raise
    
    def get_latest_records(self, interval: str = '1h', n: int = 100) -> pd.DataFrame:
        """
        Get the latest N records
        
        Args:
            interval: Time interval
            n: Number of records to return
            
        Returns:
            DataFrame with latest N records (filtered by step size)
        """
        if interval not in self.INTERVALS:
            raise ValueError(f"Invalid interval. Must be one of: {list(self.INTERVALS.keys())}")
        
        table_name = self.INTERVALS[interval]
        interval_ms = self.INTERVAL_TO_MS[interval]
        
        query = f"""
            SELECT 
                timestamp_dt,
                ts_ms,
                open,
                high,
                low,
                close
            FROM {table_name}
            WHERE ts_ms % {interval_ms} = 0
            ORDER BY ts_ms DESC
            LIMIT {n}
        """
        
        try:
            df = pd.read_sql(query, self.conn)
            
            # Convert timestamp_dt to datetime
            if not pd.api.types.is_datetime64_any_dtype(df['timestamp_dt']):
                df['timestamp_dt'] = pd.to_datetime(df['timestamp_dt'])
            
            # Sort ascending for plotting
            df = df.sort_values('ts_ms', ascending=True).reset_index(drop=True)
            
            # Calculate window start time: time = timestamp_dt - interval
            interval_delta = self.INTERVAL_TO_TIMEDELTA[interval]
            df['time'] = df['timestamp_dt'] - interval_delta
            
            # Reorder columns
            df = df[['time', 'timestamp_dt', 'ts_ms', 'open', 'high', 'low', 'close']]
            
            return df
        
        except Exception as e:
            st.error(f"Failed to load latest records: {e}")
            raise
    
    def get_statistics(self, interval: str = '1h') -> dict:
        """
        Get basic statistics for a given interval
        
        Args:
            interval: Time interval
            
        Returns:
            Dictionary with statistics
        """
        if interval not in self.INTERVALS:
            raise ValueError(f"Invalid interval. Must be one of: {list(self.INTERVALS.keys())}")
        
        table_name = self.INTERVALS[interval]
        interval_ms = self.INTERVAL_TO_MS[interval]
        
        query = f"""
            SELECT 
                COUNT(*) as total_records,
                MIN(timestamp_dt) as first_record,
                MAX(timestamp_dt) as last_record,
                MIN(low) as all_time_low,
                MAX(high) as all_time_high
            FROM {table_name}
            WHERE ts_ms % {interval_ms} = 0
        """
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                return {
                    'total_records': result[0],
                    'first_record': result[1],
                    'last_record': result[2],
                    'all_time_low': result[3],
                    'all_time_high': result[4]
                }
            else:
                return {}
        
        except Exception as e:
            st.error(f"Failed to get statistics: {e}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


# Example usage
if __name__ == "__main__":
    # This will only work when run within Streamlit context
    with DataLoader() as loader:
        # Get latest 100 records
        df = loader.get_latest_records(interval='1h', n=100)
        print(f"Loaded {len(df)} records")
        print(df.head())
        
        # Get date range
        min_date, max_date = loader.get_available_date_range(interval='1h')
        print(f"Date range: {min_date} to {max_date}")
        
        # Get statistics
        stats = loader.get_statistics(interval='1h')
        print(f"Statistics: {stats}")
