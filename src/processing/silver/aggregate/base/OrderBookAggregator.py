import polars as pl
from typing import Optional
from datetime import datetime, timedelta, timezone
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp
import gc


class OrderBookAggregator:
    """
    OrderBook Aggregator using Pre-sort + Binary Search + Sliding Window
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
        
        # Map column names
        if "created_time" not in schema.names() and "ts" in schema.names():
            df_lazy = df_lazy.with_columns([
                pl.col("ts").alias("created_time"),
                pl.col("ts").alias("time")
            ])
        
        return df_lazy.sort("created_time").collect()
    
    def create_time_index(self, df_sorted: pl.DataFrame) -> np.ndarray:
        """Create numpy array for binary search"""
        return df_sorted["created_time"].to_numpy()
    
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

    def prepare_features_sanitized(self, df_raw: pl.LazyFrame) -> pl.LazyFrame:
        """Prepare features similar to 2_aggregate_orderBook.py (no action filter),
        using simple json_decode + list.eval pattern for stability."""
        schema = df_raw.collect_schema()
        if "time" not in schema.names() and "ts" in schema.names():
            df_raw = df_raw.with_columns([
                pl.col("ts").alias("time"),
                pl.col("ts").alias("created_time")
            ])

        # Filter snapshot actions only (stabilize structure)
        try:
            if "action" in df_raw.collect_schema().names():
                df_raw = df_raw.filter(pl.col("action") == "snapshot")
        except Exception:
            pass

        df_features = df_raw.with_columns([
            pl.from_epoch(pl.col("time"), time_unit="ms").alias("timestamp_dt"),
            # Decode JSON to list-of-list of strings, then cast inner to Float64
            pl.col("bids").str.json_decode(dtype=pl.List(pl.List(pl.String))).list.eval(
                pl.element().list.eval(pl.element().cast(pl.Float64))
            ).alias("bids_float"),
            pl.col("asks").str.json_decode(dtype=pl.List(pl.List(pl.String))).list.eval(
                pl.element().list.eval(pl.element().cast(pl.Float64))
            ).alias("asks_float"),
        ]).with_columns([
            # Best bid/ask
            pl.when(pl.col("bids_float").list.len() > 0)
             .then(pl.col("bids_float").list.first().list.first())
             .otherwise(pl.lit(None, dtype=pl.Float64)).alias("best_bid"),
            pl.when(pl.col("asks_float").list.len() > 0)
             .then(pl.col("asks_float").list.first().list.first())
             .otherwise(pl.lit(None, dtype=pl.Float64)).alias("best_ask"),
            # Depth sums
            pl.col("bids_float").list.slice(0, 50).list.eval(
                pl.element().list.get(1).fill_null(0.0)
            ).list.sum().cast(pl.Float64).fill_null(0.0).alias("sum_bid_50"),
            pl.col("asks_float").list.slice(0, 50).list.eval(
                pl.element().list.get(1).fill_null(0.0)
            ).list.sum().cast(pl.Float64).fill_null(0.0).alias("sum_ask_50"),
            pl.col("bids_float").list.slice(0, 5).list.eval(
                pl.element().list.get(1).fill_null(0.0)
            ).list.sum().cast(pl.Float64).fill_null(0.0).alias("sum_bid_5"),
            pl.col("asks_float").list.slice(0, 5).list.eval(
                pl.element().list.get(1).fill_null(0.0)
            ).list.sum().cast(pl.Float64).fill_null(0.0).alias("sum_ask_5"),
            pl.col("bids_float").list.slice(0, 20).list.eval(
                pl.element().list.get(1).fill_null(0.0)
            ).list.sum().cast(pl.Float64).fill_null(0.0).alias("sum_bid_20"),
            pl.col("asks_float").list.slice(0, 20).list.eval(
                pl.element().list.get(1).fill_null(0.0)
            ).list.sum().cast(pl.Float64).fill_null(0.0).alias("sum_ask_20"),
            # Price at depth 20 with guard
            pl.when(pl.col("bids_float").list.len() > 19)
             .then(pl.col("bids_float").list.get(19).list.first())
             .otherwise(pl.when(pl.col("bids_float").list.len() > 0)
              .then(pl.col("bids_float").list.first().list.first())
              .otherwise(pl.lit(None, dtype=pl.Float64))
             ).alias("bid_px_20"),
            pl.when(pl.col("asks_float").list.len() > 19)
             .then(pl.col("asks_float").list.get(19).list.first())
             .otherwise(pl.when(pl.col("asks_float").list.len() > 0)
              .then(pl.col("asks_float").list.first().list.first())
              .otherwise(pl.lit(None, dtype=pl.Float64))
             ).alias("ask_px_20"),
            # Total money sums per level then sum
            pl.col("bids_float").list.slice(0, 50).list.eval(
                (pl.element().list.get(0).fill_null(0.0) * pl.element().list.get(1).fill_null(0.0))
            ).list.sum().cast(pl.Float64).fill_null(0.0).alias("total_money_bid_50"),
            pl.col("asks_float").list.slice(0, 50).list.eval(
                (pl.element().list.get(0).fill_null(0.0) * pl.element().list.get(1).fill_null(0.0))
            ).list.sum().cast(pl.Float64).fill_null(0.0).alias("total_money_ask_50"),
        ]).with_columns([
            (pl.col("total_money_bid_50") + pl.col("total_money_ask_50")).alias("total_money_50")
        ])

        return df_features.with_columns([
            (pl.col("best_ask") - pl.col("best_bid")).alias("spread"),
            (pl.col("total_money_50") / (pl.col("sum_bid_50") + pl.col("sum_ask_50") + 1e-9)).alias("weighted_mid_price"),
            (pl.col("sum_bid_50") / (pl.col("sum_bid_50") + pl.col("sum_ask_50") + 1e-9)).alias("deep_imbalance"),
            (pl.col("sum_bid_50") + pl.col("sum_ask_50")).alias("total_depth_50"),
            ((pl.col("sum_bid_5") + pl.col("sum_ask_5")) / (pl.col("sum_bid_50") + pl.col("sum_ask_50") + 1e-9)).alias("concentration"),
            ((pl.col("ask_px_20") - pl.col("best_ask")) / (pl.col("sum_ask_20") + 1e-9)).alias("impact_slope_ask"),
            ((pl.col("best_bid") - pl.col("bid_px_20")) / (pl.col("sum_bid_20") + 1e-9)).alias("impact_slope_bid"),
            ((pl.col("total_money_50") / (pl.col("sum_bid_50") + pl.col("sum_ask_50") + 1e-9)) - ((pl.col("best_ask") + pl.col("best_bid")) / 2)).alias("book_pressure")
        ])

    def aggregate_window_data(self, window_data: pl.DataFrame, window_end_time: int) -> dict:
        """Aggregate OrderBook window data with all features preserved"""
        if window_data is None or len(window_data) == 0:
            return {
                "timestamp_dt": datetime.fromtimestamp(window_end_time/1000),
                "ts_ms": int(window_end_time),
                "snapshot_count": 0
            }
        
        try:
            agg_result = window_data.select([
                # Weighted Mid Price
                pl.col("weighted_mid_price").mean().alias("wmp_mean"),
                pl.col("weighted_mid_price").std().alias("wmp_std"),
                pl.col("weighted_mid_price").min().alias("wmp_min"),
                pl.col("weighted_mid_price").quantile(0.25).alias("wmp_0.25"),
                pl.col("weighted_mid_price").quantile(0.50).alias("wmp_0.50"),
                pl.col("weighted_mid_price").quantile(0.75).alias("wmp_0.75"),
                pl.col("weighted_mid_price").max().alias("wmp_max"),
                pl.col("weighted_mid_price").first().alias("wmp_first"),
                pl.col("weighted_mid_price").last().alias("wmp_last"),
                pl.col("weighted_mid_price").skew().alias("wmp_skew"),
                pl.col("weighted_mid_price").kurtosis().alias("wmp_kurtosis"),

                # Spread
                pl.col("spread").mean().alias("spread_mean"),
                pl.col("spread").std().alias("spread_std"),
                pl.col("spread").min().alias("spread_min"),
                pl.col("spread").quantile(0.25).alias("spread_0.25"),
                pl.col("spread").quantile(0.50).alias("spread_0.50"),
                pl.col("spread").quantile(0.75).alias("spread_0.75"),
                pl.col("spread").max().alias("spread_max"),
                pl.col("spread").first().alias("spread_first"),
                pl.col("spread").last().alias("spread_last"),
                pl.col("spread").skew().alias("spread_skew"),
                pl.col("spread").kurtosis().alias("spread_kurtosis"),

                # Imbalance
                pl.col("deep_imbalance").mean().alias("imbal_mean"),
                pl.col("deep_imbalance").std().alias("imbal_std"),
                pl.col("deep_imbalance").min().alias("imbal_min"),
                pl.col("deep_imbalance").quantile(0.25).alias("imbal_0.25"),
                pl.col("deep_imbalance").quantile(0.50).alias("imbal_0.50"),
                pl.col("deep_imbalance").quantile(0.75).alias("imbal_0.75"),
                pl.col("deep_imbalance").max().alias("imbal_max"),
                pl.col("deep_imbalance").first().alias("imbal_first"),
                pl.col("deep_imbalance").last().alias("imbal_last"),
                pl.col("deep_imbalance").skew().alias("imbal_skew"),
                pl.col("deep_imbalance").kurtosis().alias("imbal_kurtosis"),

                # Depth & Concentration
                pl.col("total_depth_50").mean().alias("depth_mean"),
                pl.col("total_depth_50").std().alias("depth_std"),
                pl.col("total_depth_50").min().alias("depth_min"),
                pl.col("total_depth_50").max().alias("depth_max"),
                pl.col("total_depth_50").first().alias("depth_first"),
                pl.col("total_depth_50").last().alias("depth_last"),
                
                pl.col("concentration").mean().alias("conc_mean"),
                pl.col("concentration").std().alias("conc_std"),
                pl.col("concentration").min().alias("conc_min"),
                pl.col("concentration").max().alias("conc_max"),

                # Impact Slope
                pl.col("impact_slope_ask").mean().alias("impact_ask_mean"),
                pl.col("impact_slope_ask").std().alias("impact_ask_std"),
                pl.col("impact_slope_ask").min().alias("impact_ask_min"),
                pl.col("impact_slope_ask").max().alias("impact_ask_max"),
                
                pl.col("impact_slope_bid").mean().alias("impact_bid_mean"),
                pl.col("impact_slope_bid").std().alias("impact_bid_std"),
                pl.col("impact_slope_bid").min().alias("impact_bid_min"),
                pl.col("impact_slope_bid").max().alias("impact_bid_max"),

                # Book Pressure
                pl.col("book_pressure").sum().alias("pressure_sum"),
                pl.col("book_pressure").mean().alias("pressure_mean"),
                pl.col("book_pressure").std().alias("pressure_std"),
                pl.col("book_pressure").min().alias("pressure_min"),
                pl.col("book_pressure").quantile(0.25).alias("pressure_0.25"),
                pl.col("book_pressure").quantile(0.50).alias("pressure_0.50"),
                pl.col("book_pressure").quantile(0.75).alias("pressure_0.75"),
                pl.col("book_pressure").max().alias("pressure_max"),
                pl.col("book_pressure").first().alias("pressure_first"),
                pl.col("book_pressure").last().alias("pressure_last"),
                pl.col("book_pressure").skew().alias("pressure_skew"),
                pl.col("book_pressure").kurtosis().alias("pressure_kurtosis"),

                # Count & Rate
                pl.len().alias("snapshot_count"),
                pl.col("time").diff().mean().alias("rate_mean_ms"),
                pl.col("time").diff().std().alias("rate_std_ms"),
                pl.col("time").diff().min().alias("rate_min_ms"),
                pl.col("time").diff().max().alias("rate_max_ms"),

                # Correlations
                pl.corr("weighted_mid_price", "deep_imbalance").alias("corr_wmp_imbal"),
                pl.corr("weighted_mid_price", "total_depth_50").alias("corr_wmp_depth"),
                pl.corr("weighted_mid_price", "book_pressure").alias("corr_wmp_pressure"),
                pl.corr("weighted_mid_price", (pl.col("time").max() - pl.col("time"))).alias("corr_wmp_time"),
                pl.corr("spread", "deep_imbalance").alias("corr_spread_imbal"),
                pl.corr("spread", (pl.col("impact_slope_ask") + pl.col("impact_slope_bid"))/2).alias("corr_spread_impact"),
                pl.corr("concentration", (pl.col("time").max() - pl.col("time"))).alias("corr_conc_time"),
                pl.corr("book_pressure", (pl.col("time").max() - pl.col("time"))).alias("corr_pressure_time"),

                # Meta
                pl.col("time").max().alias("last_update_time_ms")
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
            return {"timestamp_dt": datetime.utcfromtimestamp(window_end_time/1000), "ts_ms": int(window_end_time), "snapshot_count": 0}
    
    def process_window(self, df_sorted: pl.DataFrame, timestamps: np.ndarray, window_end_time: int) -> dict:
        """Process single window: binary search + aggregate"""
        window_data = self.get_window_data_slice(df_sorted, timestamps, window_end_time)
        return self.aggregate_window_data(window_data, window_end_time)

    def run_pipeline(self, df_raw: pl.LazyFrame, target_date: datetime = None, existing_ts_ms: Optional[set] = None) -> Optional[pl.DataFrame]:
        """
        Execute optimal OrderBook aggregation pipeline with early-return
        """
        df_prepared = None
        df_sorted = None
        timestamps = None
        try:
            # 1) Generate windows for target date FIRST
            window_times = self.create_windows_for_date(target_date)

            # 2) Early filter: skip windows that already exist
            skipped = 0
            if existing_ts_ms:
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

            # Prepare features only if needed (use sanitized parser)
            df_prepared = self.prepare_features_sanitized(df_raw)
            
            # Load and sort data
            df_sorted = self.load_and_sort_data(df_prepared)
            
            # Create time index
            timestamps = self.create_time_index(df_sorted)
            
            # Process windows with threading
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                results = list(executor.map(
                    lambda w: self.process_window(df_sorted, timestamps, w), 
                    window_times
                ))
            
            # Filter valid results
            valid_results = [r for r in results if r.get("snapshot_count", 0) > 0]
            
            # Logging and create DataFrame (guard empty)
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
            del df_prepared, df_sorted, timestamps
            gc.collect()

    def run_for_ts_list(self, df_raw: pl.LazyFrame, ts_ms_list: list[int]) -> Optional[pl.DataFrame]:
        """Aggregate đúng tại danh sách ts_ms (ms); chỉ trả về các cửa sổ có snapshot_count > 0."""
        if not ts_ms_list:
            return pl.DataFrame([])
        df_prepared = None
        df_sorted = None
        timestamps = None
        try:
            # Chuẩn hóa và sắp xếp dữ liệu một lần
            df_prepared = self.prepare_features_sanitized(df_raw)
            df_sorted = self.load_and_sort_data(df_prepared)
            timestamps = self.create_time_index(df_sorted)
            # Đảm bảo int
            window_times = [int(t) for t in ts_ms_list]
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                results = list(executor.map(
                    lambda w: self.process_window(df_sorted, timestamps, w),
                    window_times
                ))
            valid_results = [r for r in results if r.get("snapshot_count", 0) > 0]
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
            del df_prepared, df_sorted, timestamps
            gc.collect()

