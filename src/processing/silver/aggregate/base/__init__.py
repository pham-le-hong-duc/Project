"""
Base aggregators for different data types
"""
from .TradesAggregator import TradesAggregator
from .KlinesAggregator import KlinesAggregator
from .OrderBookAggregator import OrderBookAggregator

__all__ = ['TradesAggregator', 'KlinesAggregator', 'OrderBookAggregator']
