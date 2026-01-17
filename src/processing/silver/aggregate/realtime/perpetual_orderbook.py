"""
Perpetual OrderBook Real-time Aggregation Consumer.

Reads perpetual orderbook snapshots from Redpanda and aggregates to TimescaleDB in real-time.
Architecture: Redpanda Topic → Window Aggregation → TimescaleDB (perpetual_orderBook_5m)
"""
import sys
import os
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from processing.silver.aggregate.realtime.RealtimeAggregator import RealtimeAggregator
from processing.silver.aggregate.base import OrderBookAggregator
import polars as pl


class PerpetualOrderBookConsumer(RealtimeAggregator):
    """
    Real-time aggregation consumer for perpetual orderbook.
    
    Reuses aggregation logic from batch script (perpetual_orderbook.py).
    """
    
    def __init__(self, **kwargs):
        """Initialize perpetual orderbook consumer."""
        super().__init__(
            topic='okx-orderbook',
            data_type='perpetual_orderBook',
            symbol='btc-usdt-swap',
            timestamp_field='ts',
            dedupe_columns=None,  # No dedupe - snapshots are unique
            warmup_messages=10,  # Load 10 recent snapshots on first startup
            **kwargs
        )
        
        # Initialize aggregator for all intervals
        self.aggregator = OrderBookAggregator(interval='5m')
        print(f"✅ Initialized PerpetualOrderBookConsumer with OrderBookAggregator")
    
    def aggregate_window(self, df_window, window_ts, interval):
        """
        Aggregate perpetual orderbook records in a window.
        
        Args:
            df_window: Polars DataFrame with window data
            window_ts: Window timestamp (ms)
            interval: Interval string (e.g., '5m', '15m', '1h', '4h', '1d')
        
        Returns:
            pl.DataFrame with aggregated features (70+ columns)
        """
        try:
            # Convert to LazyFrame for aggregator
            lf = df_window.lazy()
            
            # Sort by timestamp and collect to DataFrame
            df_sorted = lf.sort('ts').collect()
            
            # Aggregate window using historical aggregator
            result_dict = self.aggregator.aggregate_window_data(df_sorted, window_ts)
            
            # Check if aggregation returned valid result
            if result_dict is None:
                return None
            
            # Convert dict to DataFrame (aggregator returns dict)
            if isinstance(result_dict, dict):
                # Check if ts_ms is valid
                if result_dict.get('ts_ms') is None:
                    return None
                result = pl.DataFrame([result_dict])
            else:
                result = result_dict
            
            return result
            
        except Exception as e:
            print(f"❌ Error in aggregate_window ({interval}): {e}")
            import traceback
            traceback.print_exc()
            return None


if __name__ == "__main__":
    consumer = PerpetualOrderBookConsumer(
        bootstrap_servers=os.getenv('REDPANDA_BOOTSTRAP_SERVERS', 'redpanda:9092'),
        db_host=os.getenv('TIMESCALE_HOST', 'localhost')
    )
    
    print("Starting Perpetual OrderBook Real-time Aggregation Consumer")
    print("Redpanda → Window Aggregation → TimescaleDB")
    consumer.consume()
