import polars as pl
from typing import Optional
from datetime import datetime, timedelta, timezone
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp
import gc


class KlinesAggregator:
    """
    Klines aggregator using Pre-sort + Binary Search + Sliding Window
    """
    @staticmethod
    def _parse_interval_to_ms(interval: str) -> int:
        """Convert interval string to milliseconds"""
        unit = interval[-1].lower()
        try:
            value = int(interval[:-1])
        except ValueError:
            return 60 * 1000 

        if unit == 'm':
            return value * 60 * 1000
        elif unit == 'h':
            return value * 60 * 60 * 1000
        elif unit == 'd':
            return value * 24 * 60 * 60 * 1000
        else:
            return 60 * 1000
    
    def __init__(self, interval: str = "5m", step_size: str = "5m", max_workers: int = None):
        self.interval = interval
        self.step_size = step_size
        self.interval_ms = self._parse_interval_to_ms(interval)
        self.step_ms = self._parse_interval_to_ms(step_size)
        self.max_workers = max_workers or mp.cpu_count()
    
    def load_and_sort_data(self, df_lazy: pl.LazyFrame) -> pl.DataFrame:
        """Load and sort data once"""
        schema = df_lazy.collect_schema()
        
        # Map column names if needed
        if "open_time" in schema.names():
            df_lazy = df_lazy.with_columns([
                pl.col("open_time").alias("timestamp"),  # Keep for backward compatibility
                # Add close_time = open_time + 1 minute (since source data is 1m klines)
                (pl.col("open_time") + 60 * 1000).alias("close_time")
            ])
        
        return df_lazy.sort("close_time").collect()
    
    def create_time_index(self, df_sorted: pl.DataFrame) -> np.ndarray:
        """Create numpy array for binary search using close_time"""
        return df_sorted["close_time"].to_numpy()
    
    def create_windows_for_date(self, target_date: datetime) -> list:
        """Generate window timestamps for target date (UTC)"""
        from datetime import timezone
        
        start_of_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        next_day = target_date + timedelta(days=1)
        end_of_day = next_day.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        
        start_ms = int(start_of_day.timestamp() * 1000)
        end_ms = int(end_of_day.timestamp() * 1000)
        
        windows = []
        current_window_end = start_ms + self.step_ms
        
        while current_window_end <= end_ms:
            windows.append(current_window_end)
            current_window_end += self.step_ms
            
        return windows
    
    def get_window_data_slice(self, df_sorted: pl.DataFrame, timestamps: np.ndarray, window_end_time: int):
        """Get window data using binary search"""
        window_start_time = window_end_time - self.interval_ms
        
        start_idx = np.searchsorted(timestamps, window_start_time, side='left')
        end_idx = np.searchsorted(timestamps, window_end_time, side='left')
        
        if start_idx >= end_idx:
            return None
            
        return df_sorted[start_idx:end_idx]

    def aggregate_window_data(self, window_data: pl.DataFrame, window_end_time: int) -> dict:
        """Aggregate window data with OHLC features"""
        if window_data is None or len(window_data) == 0:
            return {
                "timestamp_dt": datetime.fromtimestamp(window_end_time/1000),
                "ts_ms": int(window_end_time)
            }
        
        try:
            agg_result = window_data.select([
                # Reconstructed OHLC
                pl.col("open").first().alias("open"),       # Giá mở cửa window
                pl.col("high").max().alias("high"),         # Giá cao nhất window
                pl.col("low").min().alias("low"),           # Giá thấp nhất window
                pl.col("close").last().alias("close"),      # Giá đóng cửa window
                
                # Stats
                pl.col("close").mean().alias("mean"),       # Giá trung bình
                pl.col("close").std().alias("std"),         # Độ biến động
            ])
            
            result = agg_result.to_dict(as_series=False)
            result["timestamp_dt"] = datetime.utcfromtimestamp(window_end_time/1000)
            result["ts_ms"] = int(window_end_time)
            
            # Extract single values from lists
            for key, value in result.items():
                if isinstance(value, list) and len(value) == 1:
                    result[key] = value[0]
                    
            return result
            
        except Exception as e:
            return {"timestamp_dt": datetime.fromtimestamp(window_end_time/1000), "ts_ms": int(window_end_time)}
    
    def process_window(self, df_sorted: pl.DataFrame, timestamps: np.ndarray, window_end_time: int) -> dict:
        """Process single window: binary search + aggregate"""
        window_data = self.get_window_data_slice(df_sorted, timestamps, window_end_time)
        return self.aggregate_window_data(window_data, window_end_time)

    def run_pipeline(self, df_prepared: pl.LazyFrame, target_date: datetime = None, existing_ts_ms: Optional[set] = None) -> Optional[pl.DataFrame]:
        """
        Execute optimal aggregation pipeline with early-return
        """
        df_sorted = None
        timestamps = None
        try:
            # 1) Generate windows for target date FIRST
            window_times = self.create_windows_for_date(target_date)

            # 2) Early filter: skip windows that already exist
            skipped = 0
            if existing_ts_ms:
                # existing_ts_ms is a set of epoch ms
                window_times_ms = [int(w if w >= 10**12 else w*1000) for w in window_times]
                skipped_idx = [i for i, wms in enumerate(window_times_ms) if wms in existing_ts_ms]
                if skipped_idx:
                    print(f"{self.interval}: Skip {len(skipped_idx)} windows")
                skipped = len(skipped_idx)
                window_times = [w for i, w in enumerate(window_times) if i not in skipped_idx]

            # 3) Early return with summary log
            if not window_times:
                skipped = len(self.create_windows_for_date(target_date))
                return pl.DataFrame([])

            # 4) Load and sort data only if needed
            df_sorted = self.load_and_sort_data(df_prepared)
            
            # 5) Create time index
            timestamps = self.create_time_index(df_sorted)
            
            # 6) Process windows with threading
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                results = list(executor.map(
                    lambda w: self.process_window(df_sorted, timestamps, w), 
                    window_times
                ))
            
            # 7) Filter valid results (has OHLC data)
            valid_results = [r for r in results if "open" in r and r["open"] is not None]
            
            # 8) Logging and create DataFrame (guard empty)
            valid_count = len(valid_results)
            if valid_count == 0:
                return pl.DataFrame([])
            final_df = pl.DataFrame(valid_results)
            if "ts_ms" in final_df.columns:
                final_df = final_df.sort("ts_ms")
            
            return final_df
            
        except Exception as e:
            print(f"Error in run_pipeline: {e}")
            return None
        finally:
            del df_sorted, timestamps
            gc.collect()

    def run_for_ts_list(self, df_prepared: pl.LazyFrame, ts_ms_list: list[int]) -> Optional[pl.DataFrame]:
        """Aggregate đúng tại danh sách window_end ts (ms); chỉ trả về các cửa sổ có OHLC hợp lệ."""
        if not ts_ms_list:
            return pl.DataFrame([])
        df_sorted = None
        timestamps = None
        try:
            df_sorted = self.load_and_sort_data(df_prepared)
            timestamps = self.create_time_index(df_sorted)
            window_times = [int(t) for t in ts_ms_list]
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                results = list(executor.map(
                    lambda w: self.process_window(df_sorted, timestamps, w),
                    window_times
                ))
            valid_results = [r for r in results if "open" in r and r["open"] is not None]
            if not valid_results:
                return pl.DataFrame([])
            df = pl.DataFrame(valid_results)
            if "ts_ms" in df.columns:
                df = df.sort("ts_ms")
            return df
        except Exception as e:
            print(f"Error in run_for_ts_list: {e}")
            return None
        finally:
            del df_sorted, timestamps
            gc.collect()
