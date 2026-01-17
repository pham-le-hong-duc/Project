"""
Spot Trades Real-time Aggregation Consumer.

Reads spot trades from Redpanda and aggregates to TimescaleDB in real-time.
Architecture: Redpanda Topic → Window Aggregation → TimescaleDB (spot_trades_5m)
"""
import sys
import os
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from processing.silver.aggregate.realtime.RealtimeAggregator import RealtimeAggregator
from processing.silver.aggregate.base import TradesAggregator
import polars as pl


class SpotTradesConsumer(RealtimeAggregator):
    """
    Real-time aggregation consumer for spot trades.
    
    Reuses aggregation logic from batch script (spot_trades.py).
    """
    
    def __init__(self, **kwargs):
        """Initialize spot trades consumer."""
        super().__init__(
            topic='okx-spot-trades',
            data_type='spot_trades',
            symbol='btc-usdt',
            timestamp_field='created_time',
            dedupe_columns=['trade_id'],  # Dedupe by trade_id
            warmup_messages=1000,  # Load 1000 recent trades on first startup
            **kwargs
        )
        
        # Initialize aggregators for all intervals
        # Note: TradesAggregator is interval-agnostic, just need one instance
        self.aggregator = TradesAggregator(interval='5m')
        print(f"✅ Initialized SpotTradesConsumer with TradesAggregator")
    
    def aggregate_window(self, df_window, window_ts, interval):
        """
        Aggregate spot trades records in a window.
        
        Args:
            df_window: Polars DataFrame with window data
            window_ts: Window timestamp (ms)
            interval: Interval string (e.g., '5m', '15m', '1h', '4h', '1d')
        
        Returns:
            pl.DataFrame with aggregated features (80+ columns)
        """
        try:
            # Convert to LazyFrame for aggregator
            lf = df_window.lazy()
            
            # Sort by timestamp and collect to DataFrame
            df_sorted = lf.sort('created_time').collect()
            
            # Aggregate window using historical aggregator
            # The aggregator works for all intervals
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
    consumer = SpotTradesConsumer(
        bootstrap_servers=os.getenv('REDPANDA_BOOTSTRAP_SERVERS', 'redpanda:9092'),
        db_host=os.getenv('TIMESCALE_HOST', 'localhost')
    )
    
    print("Starting Spot Trades Real-time Aggregation Consumer")
    print("Redpanda → Window Aggregation → TimescaleDB")
    consumer.consume()
