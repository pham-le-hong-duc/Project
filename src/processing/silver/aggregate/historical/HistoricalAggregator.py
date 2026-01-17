"""
HistoricalAggregator class for gap detection and filling
"""
import sys
sys.path.append('src')

from datetime import datetime, timezone, timedelta
from typing import List, Dict
import polars as pl

INTERVAL_TO_MS: Dict[str, int] = {
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000
}

DATA_TYPE_TO_SYMBOL: Dict[str, str] = {
    "spot_trades": "btc-usdt",
    "perpetual_trades": "btc-usdt-swap",
    "perpetual_orderBook": "btc-usdt-swap",
    "perpetual_markPriceKlines": "btc-usdt-swap",
    "perpetual_fundingRate": "btc-usdt-swap",
    "indexPriceKlines": "btc-usdt"
}


class HistoricalAggregator:
    def __init__(self, 
                 data_type: str,
                 aggregator_class,
                 base_start_date: datetime = datetime(2025, 1, 1, tzinfo=timezone.utc),
                 step_size: str = '5m',
                 intervals: List[str] = ["5m", "15m", "1h", "4h", "1d"],
                 host: str = None,
                 port: int = 5432,
                 database: str = 'okx',
                 user: str = 'okx_user',
                 password: str = 'okx_password',
                 s3_endpoint: str = None,
                 s3_access_key: str = 'admin',
                 s3_secret_key: str = 'password',
                 s3_bucket: str = 'okx',
                 s3_secure: bool = False):
        import os
        
        self.base_start_date = base_start_date
        self.data_type = data_type
        self.aggregator_class = aggregator_class
        self.step_size = step_size
        self.intervals = intervals
        
        # Use environment variables for Docker compatibility
        self.host = host or os.getenv('TIMESCALE_HOST', 'localhost')
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        
        # S3/MinIO endpoint: use environment variable or default
        self.s3_endpoint = s3_endpoint or os.getenv('MINIO_ENDPOINT', 'minio:9000')
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.s3_bucket = s3_bucket
        self.s3_secure = s3_secure
        
        self.missing_ts = {interval: {} for interval in self.intervals}
        self.max_ts_ms = {interval: None for interval in self.intervals}
        self._s3_client = None
        self._ts_client = None
    
    def _get_ts_client(self):
        """Get or create TimescaleDB client (reuse connection)"""
        if self._ts_client is None:
            from utils.timescaledb_client import TimescaleDBClient
            self._ts_client = TimescaleDBClient(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
        return self._ts_client
    
    def _get_s3_client(self):
        """Get or create S3 client (reuse connection)"""
        if self._s3_client is None:
            from utils.s3_client import MinIOWriter
            self._s3_client = MinIOWriter(
                endpoint=self.s3_endpoint,
                access_key=self.s3_access_key,
                secret_key=self.s3_secret_key,
                bucket=self.s3_bucket,
                secure=self.s3_secure
            )
        return self._s3_client
    
    def close(self):
        """Close all connections"""
        if self._ts_client is not None:
            self._ts_client.close()
            self._ts_client = None
        if self._s3_client is not None:
            # MinIO client doesn't need explicit close
            self._s3_client = None
    
    def _group_timestamps_by_date(self, timestamps: set, interval: str):
        """Helper method to group timestamps by date"""
        for ts in timestamps:
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            date_key = dt.strftime('%Y-%m-%d')
            if date_key not in self.missing_ts[interval]:
                self.missing_ts[interval][date_key] = set()
            self.missing_ts[interval][date_key].add(ts)
    
    def detect_gaps(self, interval: str):
        table_name = f"{self.data_type}_{interval}".lower()
        
        try:
            ts_client = self._get_ts_client()
            
            if not ts_client.table_exists(table_name):
                return {"error": f"Table '{table_name}' does not exist"}
            
            # Get column schema
            schema_result = ts_client.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """)
            
            if not schema_result:
                return {"error": "Could not get table schema"}
            
            all_columns = [row[0] for row in schema_result]
            data_columns = [col for col in all_columns if col not in ('ts_ms', 'timestamp_dt')]
            
            # Query complete records only
            if data_columns:
                null_conditions = " AND ".join([f'"{col}" IS NOT NULL' for col in data_columns])
                query = f"SELECT ts_ms FROM {table_name} WHERE {null_conditions} ORDER BY ts_ms"
            else:
                query = f"SELECT ts_ms FROM {table_name} ORDER BY ts_ms"
            
            result = ts_client.execute(query)
            
            if not result:
                existing_ts = set()
            else:
                existing_ts = set(row[0] for row in result)
            
            # Generate expected timestamps
            step_ms = INTERVAL_TO_MS.get(self.step_size, 5 * 60 * 1000)
            start_ms = int(self.base_start_date.timestamp() * 1000)
            # Always use current time (UTC) as max_ts_ms to check all gaps up to now
            max_ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            
            expected_ts = set(range(start_ms, max_ts_ms + 1, step_ms))
            missing_ts = expected_ts - existing_ts
            
            # Store results
            self.max_ts_ms[interval] = max_ts_ms
            self._group_timestamps_by_date(missing_ts, interval)
            
            return {"success": True, "missing": len(missing_ts), "existing": len(existing_ts)}
            
        except Exception as e:
            print(f"  ERROR: {e}")
            return {"error": str(e)}
    
    def propagate_missing_ts(self):
        step_5m = INTERVAL_TO_MS['5m']
        interval_order = ['5m', '15m', '1h', '4h', '1d']
        
        for source_idx, source_interval in enumerate(interval_order[:-1]):
            missing_source = set()
            for date_dict in self.missing_ts.get(source_interval, {}).values():
                missing_source.update(date_dict)
            
            if not missing_source:
                continue
            
            for target_interval in interval_order[source_idx + 1:]:
                target_ms = INTERVAL_TO_MS[target_interval]
                num_5m_windows = target_ms // step_5m
                max_ts_target = self.max_ts_ms.get(target_interval)
                
                affected_ts = set()
                for missing_x in missing_source:
                    for i in range(num_5m_windows):
                        new_ts = missing_x + i * step_5m
                        if max_ts_target is None or new_ts <= max_ts_target:
                            affected_ts.add(new_ts)
                
                self._group_timestamps_by_date(affected_ts, target_interval)
        
    
    def detect_all_gaps_and_propagate(self):
        # Initialize connection first
        self._get_ts_client()
        
        print(f"{'='*60}")
        print(f"GAP DETECTION: {self.data_type}")
        print(f"{'='*60}")
        
        for interval in self.intervals:
            result = self.detect_gaps(interval)
            if "error" in result:
                print(f"[{interval}] ERROR: {result['error']}")
        
        self.propagate_missing_ts()
        
        # Print summary
        for interval in self.intervals:
            count = sum(len(ts_set) for ts_set in self.missing_ts.get(interval, {}).values())
            num_days = len(self.missing_ts.get(interval, {}))
            if count > 0:
                print(f"  {interval}: {count} gaps in {num_days} days")
        
        return self.missing_ts
    
    def fill_gaps(self):
        """Fill missing timestamps by re-aggregating data from S3 and writing to TimescaleDB."""
        print(f"{'='*60}")
        print(f"FILLING GAPS: {self.data_type}")
        print(f"{'='*60}")
        
        ts_client = self._get_ts_client()
        s3_client = self._get_s3_client()
        
        for interval in self.intervals:
            date_dict = self.missing_ts.get(interval, {})
            
            if not date_dict:
                continue
            
            print(f"[{interval}] Processing {len(date_dict)} days")
            aggregator = self.aggregator_class(interval=interval, step_size=self.step_size, max_workers=4)
            
            for date_str in sorted(date_dict.keys()):
                missing_ts_list = sorted(list(date_dict[date_str]))
                missing_count = len(missing_ts_list)
                print(f"  {date_str}: {missing_count} gaps", end=" ")
                
                current_date = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                previous_date = current_date - timedelta(days=1)
                
                symbol = DATA_TYPE_TO_SYMBOL.get(self.data_type)
                if symbol is None:
                    print("ERROR: Unknown data type")
                    continue
                
                current_path = f"{self.data_type}/{symbol}/{date_str}.parquet"
                previous_path = f"{self.data_type}/{symbol}/{previous_date.strftime('%Y-%m-%d')}.parquet"
                
                current_df = s3_client.read_parquet(current_path)
                if current_df is None:
                    print("ERROR: File not found")
                    continue
                
                previous_df = s3_client.read_parquet(previous_path)
                
                if previous_df is not None:
                    merged_df = pl.concat([previous_df, current_df], how="vertical")
                else:
                    merged_df = current_df
                
                result_df = aggregator.run_for_ts_list(merged_df.lazy(), missing_ts_list)
                
                if result_df is None or len(result_df) == 0:
                    print("ERROR: Aggregation failed")
                    continue
                
                table_name = f"{self.data_type}_{interval}".lower()
                
                try:
                    rows_inserted = ts_client.upsert_dataframe(result_df, table_name, key_column="ts_ms")
                    print(f"-> upserted {rows_inserted} rows")
                except Exception as e:
                    print(f"ERROR: {e}")
        
        print(f"{'='*60}")
        print(f"COMPLETED")
        print(f"{'='*60}")
