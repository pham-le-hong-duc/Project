"""
Perpetual Mark Price Klines Consumer: Redpanda → MinIO

Reads perpetual mark price klines from Redpanda topic and writes to MinIO.
"""
import sys
from pathlib import Path

from src.ingestion.common.redpanda.Consumer import Consumer


import os
if __name__ == "__main__":
    consumer = Consumer(
        topic='okx-perpetual_markPriceKlines',
        data_type='perpetual_markPriceKlines',
        symbol='btc-usdt-swap',
        unique_field='open_time',
        timestamp_field='open_time',
        file_pattern='daily',
        bootstrap_servers=os.getenv('REDPANDA_BOOTSTRAP_SERVERS', 'redpanda:9092'),
        batch_size=1,
        batch_timeout=5
    )
    
    print("Starting Perpetual Mark Price Klines Consumer (Redpanda → MinIO)")
    consumer.consume()
