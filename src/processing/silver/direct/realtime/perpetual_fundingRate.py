"""
Perpetual Funding Rate Realtime Processing (Direct Copy - No Aggregation)
Stream funding rate data from Redpanda to TimescaleDB without aggregation
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from kafka import KafkaConsumer
import polars as pl
import json
from utils.timescaledb_client import TimescaleDBClient


class FundingRateConsumer:
    """
    Simple consumer that copies funding rate data directly from Redpanda to TimescaleDB.
    No aggregation - just passthrough.
    """
    
    def __init__(self):
        self.topic = 'okx-perpetual-fundingrate'
        self.group_id = 'silver-perpetual_fundingrate-btc-usdt-swap'
        self.table_name = 'perpetual_fundingrate'
        self.batch_size = 100
        
        # Initialize TimescaleDB client
        self.ts_client = TimescaleDBClient(
            host='timescaledb',  # Docker network
            port=5432,
            database='okx',
            user='okx_user',
            password='okx_password'
        )
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers='redpanda:9092',
            group_id=self.group_id,
            auto_offset_reset='latest',
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        print("="*60)
        print("PERPETUAL FUNDING RATE - REALTIME CONSUMER")
        print("="*60)
        print(f"Topic: {self.topic}")
        print(f"Table: {self.table_name}")
        print(f"Batch Size: {self.batch_size}")
        print("="*60)
    
    def consume(self):
        """Consume messages and write directly to TimescaleDB"""
        buffer = []
        
        try:
            for message in self.consumer:
                buffer.append(message.value)
                
                # Write batch when buffer is full
                if len(buffer) >= self.batch_size:
                    self._write_batch(buffer)
                    buffer = []
                    self.consumer.commit()
                    
        except KeyboardInterrupt:
            print("\nShutdown signal received")
            # Write remaining buffer
            if buffer:
                self._write_batch(buffer)
            self.consumer.close()
            self.ts_client.close()
    
    def _write_batch(self, records):
        """Write batch to TimescaleDB"""
        try:
            # Convert to DataFrame
            df = pl.DataFrame(records)
            
            # Upsert to TimescaleDB
            rows = self.ts_client.upsert_dataframe(df, self.table_name, key_column="funding_time")
            
            print(f"Copied {rows} funding rate records")
            
        except Exception as e:
            print(f"Error writing batch: {e}")


def main():
    consumer = FundingRateConsumer()
    consumer.consume()


if __name__ == "__main__":
    main()
