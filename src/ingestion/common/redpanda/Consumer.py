"""
Redpanda Consumer for OKX Data Ingestion.

This consumer reads messages from Redpanda topics, batches them, and writes to MinIO.
Architecture: Redpanda Topic → Batch Process → MinIO
"""
import json
import signal
import sys
from pathlib import Path
from datetime import datetime, timezone
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import polars as pl

from src.utils.s3_client import MinIOWriter


class Consumer:
    """
    Consumer that reads from Redpanda and writes to MinIO.
    
    Features:
    - Batch processing for efficiency
    - Automatic date-based file partitioning
    - Deduplication by unique field
    - Graceful shutdown
    """
    
    def __init__(self,
                 topic,
                 data_type,
                 symbol,
                 unique_field,
                 timestamp_field,
                 file_pattern='daily',
                 bootstrap_servers='localhost:19092',
                 group_id=None,
                 batch_size=1000,
                 batch_timeout=30,
                 minio_endpoint=None,
                 minio_access_key=None,
                 minio_secret_key=None,
                 minio_bucket="okx"):
        """
        Initialize consumer.
        
        Args:
            topic: Redpanda topic to consume from
            data_type: Data type (e.g., 'spot_trades', 'perpetual_trades')
            symbol: Trading symbol (e.g., 'BTC-USDT', 'BTC-USDT-SWAP')
            unique_field: Field to use for deduplication (e.g., 'trade_id')
            timestamp_field: Field to use for timestamp (e.g., 'created_time')
            file_pattern: 'daily' or 'monthly' file partitioning
            bootstrap_servers: Redpanda broker addresses
            group_id: Consumer group ID (default: {topic}-consumer-group)
            batch_size: Number of messages to batch before writing
            batch_timeout: Seconds before auto-flushing batch
            minio_endpoint: MinIO endpoint
            minio_access_key: MinIO access key
            minio_secret_key: MinIO secret key
            minio_bucket: MinIO bucket name
        """
        self.topic = topic
        self.data_type = data_type
        self.symbol = symbol.lower()  # Normalize to lowercase
        self.unique_field = unique_field
        self.timestamp_field = timestamp_field
        self.file_pattern = file_pattern
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        
        # Initialize MinIO writer
        self.minio_writer = MinIOWriter(
            endpoint=minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            bucket=minio_bucket,
            secure=False
        )
        print(f"✅ MinIO storage: s3://{minio_bucket}/{data_type}/")
        
        # Initialize Kafka consumer
        consumer_group_id = group_id or f"{topic}-consumer-group"
        
        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                group_id=consumer_group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                max_poll_records=500,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                request_timeout_ms=40000,
                metadata_max_age_ms=300000,
                consumer_timeout_ms=-1  # Block indefinitely
            )
            # Subscribe to topic (triggers partition assignment)
            self.consumer.subscribe([topic])
            print(f"✅ Connected to Redpanda topic: {topic}")
            print(f"   Consumer group: {consumer_group_id}")
            
            # Wait for partition assignment
            print("⏳ Waiting for partition assignment...")
            partitions = []
            max_wait = 30  # Wait up to 30 seconds
            for _ in range(max_wait):
                partitions = self.consumer.assignment()
                if partitions:
                    break
                self.consumer.poll(timeout_ms=1000)  # Trigger assignment
            
            if not partitions:
                raise Exception(f"Failed to get partition assignment after {max_wait}s")
            
            print(f"✅ Assigned partitions: {partitions}")
            
            # Display partition info (auto_offset_reset will handle offset positioning)
            for partition in partitions:
                try:
                    committed = self.consumer.committed(partition)
                    if committed is None:
                        print(f"   Partition {partition.partition}: No committed offset (will use auto_offset_reset=earliest)")
                    else:
                        print(f"   Partition {partition.partition}: Resuming from offset {committed}")
                except Exception as e:
                    print(f"   Partition {partition.partition}: Error checking offset - {e}")
        except Exception as e:
            print(f"❌ Failed to connect to Redpanda: {e}")
            raise
        
        self.running = True  # Set to True so consume loop can run
        self.batch = []
        self.last_flush_time = datetime.now(timezone.utc)
        
        # Stats
        self.total_consumed = 0
        self.total_written = 0
    
    def _group_by_period(self, records):
        """
        Group records by date or month based on file_pattern.
        
        Args:
            records: List of record dicts
        
        Returns:
            dict: {period_str: [records]}
        """
        grouped = {}
        
        for record in records:
            ts = record[self.timestamp_field]
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            
            if self.file_pattern == 'daily':
                period_str = dt.strftime('%Y-%m-%d')
            else:  # monthly
                period_str = dt.strftime('%Y-%m')
            
            if period_str not in grouped:
                grouped[period_str] = []
            grouped[period_str].append(record)
        
        return grouped
    
    def _write_to_minio(self, records):
        """
        Write records to MinIO, grouped by period.
        
        Args:
            records: List of record dicts
        """
        if not records:
            return
        
        # Group by period
        grouped = self._group_by_period(records)
        
        # Write each period
        for period_str, period_records in grouped.items():
            try:
                # Convert to DataFrame
                df = pl.DataFrame(period_records).sort(self.timestamp_field)
                
                # MinIO path (use symbol from constructor)
                object_path = f"{self.data_type}/{self.symbol}/{period_str}.parquet"
                
                # Check if file exists
                existing_df = self.minio_writer.read_parquet(object_path)
                
                if existing_df is not None:
                    # Merge and deduplicate
                    combined_df = pl.concat([existing_df, df]).unique(
                        subset=[self.unique_field],
                        keep="last"
                    ).sort(self.timestamp_field)
                    final_df = combined_df
                else:
                    final_df = df
                
                # Write to MinIO
                if self.minio_writer.write_parquet(final_df, object_path):
                    print(f"✓ {len(period_records)} records → {object_path}")
                    self.total_written += len(period_records)
                else:
                    print(f"✗ Failed to write {len(period_records)} records to {object_path}")
                    
            except Exception as e:
                print(f"❌ Error writing to MinIO: {e}")
    
    def _flush_batch(self):
        """Flush current batch to MinIO."""
        if not self.batch:
            return
        
        try:
            count = len(self.batch)
            self._write_to_minio(self.batch)
            self.batch.clear()
            self.last_flush_time = datetime.now(timezone.utc)
            
        except Exception as e:
            print(f"❌ Error flushing batch: {e}")
    
    def _should_flush(self):
        """Check if batch should be flushed based on timeout."""
        elapsed = (datetime.now(timezone.utc) - self.last_flush_time).total_seconds()
        return elapsed >= self.batch_timeout
    
    def consume(self):
        """
        Main consume loop.
        Reads messages from Redpanda and writes to MinIO in batches.
        """
        self.running = True
        
        print(f"\n{'='*80}")
        print(f"Started consuming from topic: {self.topic}")
        print(f"Batch size: {self.batch_size}, Timeout: {self.batch_timeout}s")
        print(f"{'='*80}\n")
        
        try:
            while self.running:
                # Poll for messages (timeout 1 second)
                messages = self.consumer.poll(timeout_ms=1000)
                
                if not messages:
                    # Check timeout flush
                    if self._should_flush() and self.batch:
                        self._flush_batch()
                        self.consumer.commit()
                    continue
                
                # Process messages
                for topic_partition, records in messages.items():
                    for record in records:
                        try:
                            if record.value is None:
                                print(f"⚠️ WARNING: Received None value, skipping")
                                continue
                            
                            self.batch.append(record.value)
                            self.total_consumed += 1
                            
                            # Flush if batch is full
                            if len(self.batch) >= self.batch_size:
                                self._flush_batch()
                                self.consumer.commit()
                                
                        except Exception as e:
                            print(f"❌ Error processing message: {e}")
                            import traceback
                            traceback.print_exc()
        
        except KeyboardInterrupt:
            print("\n⚠️  Received interrupt signal")
        except Exception as e:
            print(f"❌ Consumer error: {e}")
        finally:
            self._shutdown()
    
    def _shutdown(self):
        """Graceful shutdown."""
        print("\n⚠️  Shutting down consumer...")
        
        # Flush remaining batch
        if self.batch:
            self._flush_batch()
        
        # Commit offsets
        try:
            self.consumer.commit()
            print("✅ Offsets committed")
        except Exception as e:
            print(f"⚠️  Failed to commit offsets: {e}")
        
        # Close consumer
        try:
            self.consumer.close()
            print("✅ Consumer closed")
        except Exception as e:
            print(f"⚠️  Failed to close consumer: {e}")
        
        # Print stats
        print(f"\n{'='*80}")
        print("CONSUMER STATISTICS:")
        print(f"{'='*80}")
        print(f"Total consumed: {self.total_consumed}")
        print(f"Total written: {self.total_written}")
        print(f"{'='*80}\n")
    
    def stop(self):
        """Stop the consumer."""
        self.running = False


# Signal handler
consumer_instance = None

def signal_handler(sig, frame):
    """Handle interrupt signals."""
    print(f"\nReceived signal {sig}")
    if consumer_instance:
        consumer_instance.stop()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Redpanda to MinIO Consumer')
    parser.add_argument('--topic', required=True, help='Redpanda topic to consume from')
    parser.add_argument('--data-type', required=True, help='Data type (e.g., spot_trades)')
    parser.add_argument('--unique-field', required=True, help='Unique field for deduplication')
    parser.add_argument('--timestamp-field', required=True, help='Timestamp field name')
    parser.add_argument('--file-pattern', default='daily', choices=['daily', 'monthly'], 
                        help='File partitioning pattern')
    parser.add_argument('--bootstrap-servers', default='localhost:19092',
                        help='Redpanda bootstrap servers')
    parser.add_argument('--group-id', help='Consumer group ID')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size')
    parser.add_argument('--batch-timeout', type=int, default=30, help='Batch timeout in seconds')
    
    args = parser.parse_args()
    
    consumer_instance = Consumer(
        topic=args.topic,
        data_type=args.data_type,
        unique_field=args.unique_field,
        timestamp_field=args.timestamp_field,
        file_pattern=args.file_pattern,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        batch_size=args.batch_size,
        batch_timeout=args.batch_timeout
    )
    
    consumer_instance.consume()
