"""
Perpetual Trades Consumer: Redpanda → MinIO

Reads perpetual trades from Redpanda topic and writes to MinIO.
"""
import sys
from pathlib import Path

from src.ingestion.common.redpanda.Consumer import Consumer


import os
if __name__ == "__main__":
    consumer = Consumer(
        topic='okx-perpetual_trades',
        data_type='perpetual_trades',
        symbol='btc-usdt-swap',
        unique_field='trade_id',
        timestamp_field='created_time',
        file_pattern='daily',
        bootstrap_servers=os.getenv('REDPANDA_BOOTSTRAP_SERVERS', 'redpanda:9092'),
        batch_size=50,
        batch_timeout=2
    )
    
    print("Starting Perpetual Trades Consumer (Redpanda → MinIO)")
    consumer.consume()
