"""
Redpanda Producer Helper for OKX Data Ingestion.

This class provides a simple wrapper around Kafka producer for sending messages to Redpanda.
"""
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError


class Producer:
    """
    Wrapper class for producing messages to Redpanda topics.
    
    Features:
    - Automatic JSON serialization
    - Error handling
    - Async sending with callbacks
    """
    
    def __init__(self, 
                 bootstrap_servers='localhost:19092',
                 topic=None,
                 compression_type=None,
                 batch_size=16384,
                 linger_ms=10):
        """
        Initialize Redpanda producer.
        
        Args:
            bootstrap_servers: Redpanda broker addresses (default: localhost:19092)
            topic: Default topic to send messages to
            compression_type: Compression algorithm (snappy, gzip, lz4, zstd)
            batch_size: Batch size in bytes before sending
            linger_ms: Time to wait before sending batch (allows batching)
        """
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                compression_type=compression_type,
                batch_size=batch_size,
                linger_ms=linger_ms,
                acks=1,  # Wait for leader confirmation (was 0)
                retries=3,  # Retry 3 times on failure (was 0)
                max_in_flight_requests_per_connection=5,
                api_version=(0, 10, 0),  # Use older API version compatible with Redpanda
                request_timeout_ms=10000,
                metadata_max_age_ms=300000,
                buffer_memory=33554432,
                max_block_ms=5000
            )
            print(f"✅ Connected to Redpanda at {bootstrap_servers}")
        except Exception as e:
            print(f"❌ Failed to initialize Redpanda producer: {e}")
            raise
    
    def send(self, message, topic=None, key=None):
        """
        Send a message to Redpanda topic.
        
        Args:
            message: Message to send (dict, will be JSON serialized)
            topic: Topic to send to (uses default if not specified)
            key: Optional message key for partitioning
        
        Returns:
            FutureRecordMetadata: Future object for the send result
        """
        target_topic = topic or self.topic
        
        if not target_topic:
            print(f"❌ No topic specified! self.topic={self.topic}, topic param={topic}")
            raise ValueError("No topic specified and no default topic set")
        
        try:
            future = self.producer.send(target_topic, value=message, key=key)
            if future is None:
                print(f"⚠️ producer.send() returned None for topic {target_topic}")
            return future
        except KafkaError as e:
            print(f"❌ KafkaError sending message: {e}")
            import traceback
            traceback.print_exc()
            return None
        except Exception as e:
            print(f"❌ Unexpected error sending message: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def send_batch(self, messages, topic=None):
        """
        Send multiple messages to Redpanda asynchronously.
        
        Args:
            messages: List of messages (dicts)
            topic: Topic to send to (uses default if not specified)
        
        Returns:
            int: Number of messages queued (not necessarily sent)
        """
        count = 0
        for msg in messages:
            future = self.send(msg, topic=topic)
            if future:
                count += 1
            else:
                print(f"⚠️ Message failed to queue!")
        return count
    
    def flush(self, timeout=30):
        """
        Force send all buffered messages.
        
        Args:
            timeout: Max time to wait in seconds (default: 5)
        """
        try:
            self.producer.flush(timeout=timeout)
        except Exception as e:
            print(f"⚠️ Flush error: {e}")
            import traceback
            traceback.print_exc()
    
    def close(self, timeout=None):
        """
        Close the producer and flush all pending messages.
        
        Args:
            timeout: Max time to wait in seconds
        """
        try:
            self.producer.close(timeout=timeout)
            print("✅ Producer closed successfully")
        except Exception as e:
            print(f"❌ Error closing producer: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# NOTE: ProducerPool class removed - not currently used in codebase.
# If needed in future for managing multiple producers, can be restored from git history.


# Test function
if __name__ == "__main__":
    # Test single producer
    print("Testing Producer...")
    
    with Producer(topic='okx-spot-trades') as producer:
        # Send single message
        test_msg = {
            'instrument_name': 'BTC-USDT',
            'trade_id': 123456,
            'side': 'buy',
            'price': 50000.0,
            'size': 0.1,
            'created_time': 1704067200000
        }
        
        future = producer.send(test_msg)
        if future:
            # Wait for result
            try:
                record_metadata = future.get(timeout=10)
                print(f"✅ Message sent successfully!")
                print(f"   Topic: {record_metadata.topic}")
                print(f"   Partition: {record_metadata.partition}")
                print(f"   Offset: {record_metadata.offset}")
            except Exception as e:
                print(f"❌ Failed to send: {e}")
    
    print("\nTest completed!")
