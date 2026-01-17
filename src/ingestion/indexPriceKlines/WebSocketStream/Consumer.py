"""
Index Price Klines Consumer: Redpanda → MinIO

Reads index price klines from Redpanda topic and writes to MinIO.
"""
import sys
from pathlib import Path

from src.ingestion.common.redpanda.Consumer import Consumer


import os
if __name__ == "__main__":
    consumer = Consumer(
        topic='okx-index-klines',
        data_type='indexPriceKlines',
        symbol='btc-usdt',
        unique_field='open_time',
        timestamp_field='open_time',
        file_pattern='daily',
        bootstrap_servers=os.getenv('REDPANDA_BOOTSTRAP_SERVERS', 'redpanda:9092'),
        batch_size=1,
        batch_timeout=5
    )
    
    print("Starting Index Price Klines Consumer (Redpanda → MinIO)")
    consumer.consume()
