"""
Spot Trades Historical Aggregation using HistoricalAggregator
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from processing.silver.aggregate.base import TradesAggregator
from processing.silver.aggregate.historical.HistoricalAggregator import HistoricalAggregator


def process_spot_trades():
    """Process spot trades historical data - detect and fill gaps"""
    agg = HistoricalAggregator(
        data_type="spot_trades",
        aggregator_class=TradesAggregator
    )
    
    try:
        missing_ts = agg.detect_all_gaps_and_propagate()
        agg.fill_gaps()
        
        return missing_ts
    finally:
        agg.close()


if __name__ == "__main__":
    process_spot_trades()
