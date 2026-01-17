"""
Perpetual OrderBook Consumer: Redpanda → MinIO

Reads perpetual orderbook snapshots from Redpanda topic and writes to MinIO.
"""
import sys
from pathlib import Path

from src.ingestion.common.redpanda.Consumer import Consumer


import os
if __name__ == "__main__":
    consumer = Consumer(
        topic='okx-perpetual_orderBook',
        data_type='perpetual_orderBook',
        symbol='btc-usdt-swap',
        unique_field='ts',
        timestamp_field='ts',
        file_pattern='daily',
        bootstrap_servers=os.getenv('REDPANDA_BOOTSTRAP_SERVERS', 'redpanda:9092'),
        batch_size=5,
        batch_timeout=2
    )
    
    print("Starting Perpetual OrderBook Consumer (Redpanda → MinIO)")
    consumer.consume()
