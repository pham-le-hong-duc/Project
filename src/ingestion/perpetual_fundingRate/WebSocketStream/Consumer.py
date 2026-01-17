"""
Perpetual Funding Rate Consumer: Redpanda → MinIO

Reads perpetual funding rate from Redpanda topic and writes to MinIO.
Data is stored in monthly files (2025-01.parquet, 2025-02.parquet, etc.)
"""
import sys
from pathlib import Path

from src.ingestion.common.redpanda.Consumer import Consumer


import os
if __name__ == "__main__":
    consumer = Consumer(
        topic='okx-perpetual_fundingRate',
        data_type='perpetual_fundingRate',
        symbol='btc-usdt-swap',
        unique_field='funding_time',
        timestamp_field='funding_time',
        file_pattern='monthly',  # Monthly files (not daily)
        bootstrap_servers=os.getenv('REDPANDA_BOOTSTRAP_SERVERS', 'redpanda:9092'),
        batch_size=1,
        batch_timeout=5
    )
    
    print("Starting Perpetual Funding Rate Consumer (Redpanda → MinIO)")
    consumer.consume()
