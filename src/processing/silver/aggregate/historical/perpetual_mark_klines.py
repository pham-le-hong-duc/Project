"""
Perpetual Mark Price Klines Historical Aggregation using HistoricalAggregator
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from processing.silver.aggregate.base import KlinesAggregator
from processing.silver.aggregate.historical.HistoricalAggregator import HistoricalAggregator


def process_perpetual_mark_klines():
    """Process perpetual mark price klines historical data - detect and fill gaps"""
    agg = HistoricalAggregator(
        data_type="perpetual_markPriceKlines",
        aggregator_class=KlinesAggregator
    )
    
    try:
        missing_ts = agg.detect_all_gaps_and_propagate()
        agg.fill_gaps()
        
        return missing_ts
    finally:
        agg.close()


if __name__ == "__main__":
    process_perpetual_mark_klines()
