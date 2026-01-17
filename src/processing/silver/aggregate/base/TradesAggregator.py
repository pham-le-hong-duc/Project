import polars as pl
from typing import Optional
from datetime import datetime, timedelta, timezone
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp
import gc


class TradesAggregator:
    """
    Trades aggregator using Pre-sort + Binary Search + Sliding Window
    (kept identical to perpetual version)
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

        # Add required columns
        if "turnover" not in schema.names():
            df_lazy = df_lazy.with_columns((pl.col("price") * pl.col("size")).alias("turnover"))

        if "time" not in schema.names():
            df_lazy = df_lazy.with_columns(pl.col("created_time").alias("time"))

        return df_lazy.sort("created_time").collect()

    def create_time_index(self, df_sorted: pl.DataFrame) -> np.ndarray:
        """Create numpy array for binary search"""
        return df_sorted["created_time"].to_numpy()

    def create_windows_for_date(self, target_date: datetime) -> list:
        """Generate window timestamps for target date (UTC)"""
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
        """Aggregate window data with all features preserved"""
        if window_data is None or len(window_data) == 0:
            return {
                "timestamp_dt": datetime.fromtimestamp(window_end_time/1000), 
                "ts_ms": int(window_end_time),
                "trade_count": 0
            }

        try:
            # Calculate turnover if not exists (turnover = price * size)
            if 'turnover' not in window_data.columns:
                window_data = window_data.with_columns([
                    (pl.col('price') * pl.col('size')).alias('turnover')
                ])
            
            # Create 'time' alias if not exists (should be 'created_time')
            if 'time' not in window_data.columns and 'created_time' in window_data.columns:
                window_data = window_data.with_columns([
                    pl.col('created_time').alias('time')
                ])
            
            agg_result = window_data.select([
                # Volume
                pl.col("size").filter(pl.col("side") == "buy").sum().alias("volume_buy"),
                pl.col("size").filter(pl.col("side") == "sell").sum().alias("volume_sell"),

                # Turnover
                pl.col("turnover").filter(pl.col("side") == "buy").sum().alias("turnover_buy"),
                pl.col("turnover").filter(pl.col("side") == "sell").sum().alias("turnover_sell"),

                # Count
                pl.col("side").filter(pl.col("side") == "buy").count().alias("count_buy"),
                pl.col("side").filter(pl.col("side") == "sell").count().alias("count_sell"),
                (pl.col("price").diff(1) > 0).sum().alias("count_tick_up"),
                (pl.col("price").diff(1) < 0).sum().alias("count_tick_down"),

                # Price Stats - Trade
                pl.col("price").first().alias("price_first_trade"),
                pl.col("price").last().alias("price_last_trade"),
                pl.col("price").mean().alias("price_mean_trade"),
                pl.col("price").std().alias("price_std_trade"),
                pl.col("price").min().alias("price_min_trade"),
                pl.col("price").max().alias("price_max_trade"),
                pl.col("price").quantile(0.25).alias("price_0.25_trade"),
                pl.col("price").quantile(0.50).alias("price_0.50_trade"),
                pl.col("price").quantile(0.75).alias("price_0.75_trade"),
                pl.col("price").skew().alias("price_skew_trade"),
                pl.col("price").kurtosis().alias("price_kurtosis_trade"),
                pl.col("price").n_unique().alias("nunique_price_trade"),

                # Price Stats - Buy
                pl.col("price").filter(pl.col("side") == "buy").first().alias("price_first_buy"),
                pl.col("price").filter(pl.col("side") == "buy").last().alias("price_last_buy"),
                pl.col("price").filter(pl.col("side") == "buy").mean().alias("price_mean_buy"),
                pl.col("price").filter(pl.col("side") == "buy").std().alias("price_std_buy"),
                pl.col("price").filter(pl.col("side") == "buy").min().alias("price_min_buy"),
                pl.col("price").filter(pl.col("side") == "buy").max().alias("price_max_buy"),
                pl.col("price").filter(pl.col("side") == "buy").quantile(0.25).alias("price_0.25_buy"),
                pl.col("price").filter(pl.col("side") == "buy").quantile(0.50).alias("price_0.50_buy"),
                pl.col("price").filter(pl.col("side") == "buy").quantile(0.75).alias("price_0.75_buy"),
                pl.col("price").filter(pl.col("side") == "buy").skew().alias("price_skew_buy"),
                pl.col("price").filter(pl.col("side") == "buy").kurtosis().alias("price_kurtosis_buy"),
                pl.col("price").filter(pl.col("side") == "buy").n_unique().alias("nunique_price_buy"),

                # Price Stats - Sell
                pl.col("price").filter(pl.col("side") == "sell").first().alias("price_first_sell"),
                pl.col("price").filter(pl.col("side") == "sell").last().alias("price_last_sell"),
                pl.col("price").filter(pl.col("side") == "sell").mean().alias("price_mean_sell"),
                pl.col("price").filter(pl.col("side") == "sell").std().alias("price_std_sell"),
                pl.col("price").filter(pl.col("side") == "sell").min().alias("price_min_sell"),
                pl.col("price").filter(pl.col("side") == "sell").max().alias("price_max_sell"),
                pl.col("price").filter(pl.col("side") == "sell").quantile(0.25).alias("price_0.25_sell"),
                pl.col("price").filter(pl.col("side") == "sell").quantile(0.50).alias("price_0.50_sell"),
                pl.col("price").filter(pl.col("side") == "sell").quantile(0.75).alias("price_0.75_sell"),
                pl.col("price").filter(pl.col("side") == "sell").skew().alias("price_skew_sell"),
                pl.col("price").filter(pl.col("side") == "sell").kurtosis().alias("price_kurtosis_sell"),
                pl.col("price").filter(pl.col("side") == "sell").n_unique().alias("nunique_price_sell"),

                # Size Stats - Trade
                pl.col("size").mean().alias("size_mean_trade"),
                pl.col("size").std().alias("size_std_trade"),
                pl.col("size").min().alias("size_min_trade"),
                pl.col("size").max().alias("size_max_trade"),
                pl.col("size").quantile(0.25).alias("size_0.25_trade"),
                pl.col("size").quantile(0.50).alias("size_0.50_trade"),
                pl.col("size").quantile(0.75).alias("size_0.75_trade"),
                pl.col("size").skew().alias("size_skew_trade"),
                pl.col("size").kurtosis().alias("size_kurtosis_trade"),
                pl.col("size").n_unique().alias("nunique_size_trade"),

                # Size Stats - Buy
                pl.col("size").filter(pl.col("side") == "buy").mean().alias("size_mean_buy"),
                pl.col("size").filter(pl.col("side") == "buy").std().alias("size_std_buy"),
                pl.col("size").filter(pl.col("side") == "buy").min().alias("size_min_buy"),
                pl.col("size").filter(pl.col("side") == "buy").max().alias("size_max_buy"),
                pl.col("size").filter(pl.col("side") == "buy").quantile(0.25).alias("size_0.25_buy"),
                pl.col("size").filter(pl.col("side") == "buy").quantile(0.50).alias("size_0.50_buy"),
                pl.col("size").filter(pl.col("side") == "buy").quantile(0.75).alias("size_0.75_buy"),
                pl.col("size").filter(pl.col("side") == "buy").skew().alias("size_skew_buy"),
                pl.col("size").filter(pl.col("side") == "buy").kurtosis().alias("size_kurtosis_buy"),
                pl.col("size").filter(pl.col("side") == "buy").n_unique().alias("nunique_size_buy"),

                # Size Stats - Sell
                pl.col("size").filter(pl.col("side") == "sell").mean().alias("size_mean_sell"),
                pl.col("size").filter(pl.col("side") == "sell").std().alias("size_std_sell"),
                pl.col("size").filter(pl.col("side") == "sell").min().alias("size_min_sell"),
                pl.col("size").filter(pl.col("side") == "sell").max().alias("size_max_sell"),
                pl.col("size").filter(pl.col("side") == "sell").quantile(0.25).alias("size_0.25_sell"),
                pl.col("size").filter(pl.col("side") == "sell").quantile(0.50).alias("size_0.50_sell"),
                pl.col("size").filter(pl.col("side") == "sell").quantile(0.75).alias("size_0.75_sell"),
                pl.col("size").filter(pl.col("side") == "sell").skew().alias("size_skew_sell"),
                pl.col("size").filter(pl.col("side") == "sell").kurtosis().alias("size_kurtosis_sell"),
                pl.col("size").filter(pl.col("side") == "sell").n_unique().alias("nunique_size_sell"),

                # Trade Rate
                pl.col("time").diff().mean().alias("rate_mean_ms_trade"),
                pl.col("time").diff().std().alias("rate_std_ms_trade"),
                pl.col("time").diff().max().alias("rate_max_ms_trade"),
                pl.col("time").diff().min().alias("rate_min_ms_trade"),

                # Rate - Buy
                pl.col("time").filter(pl.col("side") == "buy").diff().mean().alias("rate_mean_ms_buy"),
                pl.col("time").filter(pl.col("side") == "buy").diff().std().alias("rate_std_ms_buy"),
                pl.col("time").filter(pl.col("side") == "buy").diff().max().alias("rate_max_ms_buy"),
                pl.col("time").filter(pl.col("side") == "buy").diff().min().alias("rate_min_ms_buy"),

                # Rate - Sell
                pl.col("time").filter(pl.col("side") == "sell").diff().mean().alias("rate_mean_ms_sell"),
                pl.col("time").filter(pl.col("side") == "sell").diff().std().alias("rate_std_ms_sell"),
                pl.col("time").filter(pl.col("side") == "sell").diff().max().alias("rate_max_ms_sell"),
                pl.col("time").filter(pl.col("side") == "sell").diff().min().alias("rate_min_ms_sell"),

                # Correlation
                pl.corr("price", "size").alias("corr_price_size_trade"),
                pl.corr("price", (pl.col("time").max() - pl.col("time")) / self.interval_ms).alias("corr_price_time_trade"),
                pl.corr("size", (pl.col("time").max() - pl.col("time")) / self.interval_ms).alias("corr_size_time_trade"),

                # Meta
                pl.col("time").max().alias("last_trade_time_ms"),
                pl.len().alias("trade_count"),
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
            print(f"    ERROR in aggregate_window_data: {e}")
            import traceback
            traceback.print_exc()
            return {
                "timestamp_dt": datetime.fromtimestamp(window_end_time/1000), 
                "ts_ms": int(window_end_time),
                "trade_count": 0
            }

    def process_window(self, df_sorted: pl.DataFrame, timestamps: np.ndarray, window_end_time: int) -> dict:
        """Process single window: binary search + aggregate"""
        window_data = self.get_window_data_slice(df_sorted, timestamps, window_end_time)
        return self.aggregate_window_data(window_data, window_end_time)

    def run_pipeline(self, df_prepared: pl.LazyFrame, target_date: datetime = None, existing_ts_ms: Optional[set] = None) -> Optional[pl.DataFrame]:
        """
        Execute optimal aggregation pipeline with early-return and logging to verify skip behavior
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
                skipped = len(skipped_idx)
                window_times = [w for i, w in enumerate(window_times) if i not in skipped_idx]

            # 3) Early return: nothing left to compute (log summary)
            if not window_times:
                return pl.DataFrame([])

            # 4) Load and sort data only if we have work to do
            df_sorted = self.load_and_sort_data(df_prepared)

            # 5) Create time index
            timestamps = self.create_time_index(df_sorted)

            # 6) Process windows with threading
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                results = list(executor.map(
                    lambda w: self.process_window(df_sorted, timestamps, w),
                    window_times
                ))

            # 7) Filter valid results
            valid_results = [r for r in results if r.get("trade_count", 0) > 0]

            # 8) Create DataFrame (avoid sorting empty frame)
            if not valid_results:
                return pl.DataFrame([])

            final_df = pl.DataFrame(valid_results)
            if "ts_ms" in final_df.columns:
                final_df = final_df.sort("ts_ms")

            return final_df

        except Exception:
            return None
        finally:
            del df_sorted, timestamps
            gc.collect()

    def run_for_ts_list(self, df_prepared: pl.LazyFrame, ts_ms_list: list[int]) -> Optional[pl.DataFrame]:
        """Aggregate exactly at the provided list of window_end timestamps (ms), ignoring skip logic.
        Returns only windows with trade_count > 0 (others dropped per requirement)."""
        if not ts_ms_list:
            return pl.DataFrame([])
        df_sorted = None
        timestamps = None
        try:
            df_sorted = self.load_and_sort_data(df_prepared)
            timestamps = self.create_time_index(df_sorted)
            # Ensure ints
            window_times = [int(t) for t in ts_ms_list]
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                results = list(executor.map(
                    lambda w: self.process_window(df_sorted, timestamps, w),
                    window_times
                ))
            valid_results = [r for r in results if r.get("trade_count", 0) > 0]
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

