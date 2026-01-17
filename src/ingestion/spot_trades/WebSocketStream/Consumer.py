"""
Spot Trades Consumer: Redpanda → MinIO

This consumer reads spot trades from Redpanda topic and writes to MinIO.
Run this alongside WebSocketStreamNew.py for complete data pipeline.
"""
import sys
from pathlib import Path

from src.ingestion.common.redpanda.Consumer import Consumer


if __name__ == "__main__":
    import os
    consumer = Consumer(
        topic='okx-spot_trades',
        data_type='spot_trades',
        symbol='btc-usdt',
        unique_field='trade_id',
        timestamp_field='created_time',
        file_pattern='daily',
        bootstrap_servers=os.getenv('REDPANDA_BOOTSTRAP_SERVERS', 'redpanda:9092'),
        batch_size=50,
        batch_timeout=2
    )
    
    print("Starting Spot Trades Consumer (Redpanda → MinIO)")
    consumer.consume()
