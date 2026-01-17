"""
Setup Redpanda topics for OKX data ingestion.

This script creates all necessary Kafka topics for streaming data from WebSocket to Redpanda.
Topics are created with appropriate partitions and retention policies.
"""
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import sys


def create_topics(bootstrap_servers='localhost:9092'):
    """
    Create all required Redpanda topics for OKX data ingestion.
    
    Topic naming convention: okx-{data_type}
    Partitions:
        - High volume data (trades): 3 partitions for parallel processing
        - Low volume data (funding rate, klines): 1 partition
    
    If topic already exists, it will be skipped (no deletion).
    """
    
    # Initialize Kafka admin client
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='okx-topic-creator'
        )
        print(f"Connected to Redpanda at {bootstrap_servers}")
    except Exception as e:
        print(f"❌ Failed to connect to Redpanda: {e}")
        print("Make sure Redpanda is running on localhost:9092")
        sys.exit(1)
    
    # Define topics with configurations
    topics = [
        # Spot Trades - High volume
        NewTopic(
            name='okx-spot-trades',
            num_partitions=3,
            replication_factor=1,
            topic_configs={
                'retention.ms': str(7 * 24 * 60 * 60 * 1000),  # 7 days retention
                'compression.type': 'producer'  # Let producer decide (None = no compression)
            }
        ),
        
        # Perpetual Trades - High volume
        NewTopic(
            name='okx-perpetual-trades',
            num_partitions=3,
            replication_factor=1,
            topic_configs={
                'retention.ms': str(7 * 24 * 60 * 60 * 1000),
                'compression.type': 'snappy'
            }
        ),
        
        # Perpetual Order Book - Very high volume
        NewTopic(
            name='okx-perpetual-orderbook',
            num_partitions=5,
            replication_factor=1,
            topic_configs={
                'retention.ms': str(3 * 24 * 60 * 60 * 1000),  # 3 days (high volume)
                'compression.type': 'producer'
            }
        ),
        
        # Perpetual Funding Rate - Low volume (every 8 hours)
        NewTopic(
            name='okx-perpetual-funding-rate',
            num_partitions=1,
            replication_factor=1,
            topic_configs={
                'retention.ms': str(30 * 24 * 60 * 60 * 1000),  # 30 days
                'compression.type': 'producer'
            }
        ),
        
        # Index Price Klines - Medium volume
        NewTopic(
            name='okx-index-klines',
            num_partitions=1,
            replication_factor=1,
            topic_configs={
                'retention.ms': str(7 * 24 * 60 * 60 * 1000),
                'compression.type': 'producer'
            }
        ),
        
        # Perpetual Mark Price Klines - Medium volume
        NewTopic(
            name='okx-perpetual-mark-klines',
            num_partitions=1,
            replication_factor=1,
            topic_configs={
                'retention.ms': str(7 * 24 * 60 * 60 * 1000),
                'compression.type': 'producer'
            }
        ),
    ]
    
    # Create topics
    created_count = 0
    exists_count = 0
    failed_count = 0
    
    print(f"\nCreating {len(topics)} topics...\n")
    
    for topic in topics:
        try:
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            print(f"✅ Created: {topic.name} ({topic.num_partitions} partitions)")
            created_count += 1
        except TopicAlreadyExistsError:
            print(f"✓ Exists: {topic.name} (skipped)")
            exists_count += 1
        except Exception as e:
            print(f"❌ Failed: {topic.name} - {e}")
            failed_count += 1
    
    # Close admin client
    admin_client.close()
    
    # Print summary
    print(f"\n{'='*60}")
    print("TOPIC CREATION SUMMARY:")
    print(f"{'='*60}")
    print(f"Created: {created_count}")
    print(f"Already exists: {exists_count}")
    print(f"Failed: {failed_count}")
    print(f"{'='*60}\n")
    
    # List all topics to verify
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        existing_topics = admin_client.list_topics()
        okx_topics = [t for t in existing_topics if t.startswith('okx-')]
        
        print("All OKX topics:")
        for topic in sorted(okx_topics):
            print(f"  • {topic}")
        
        admin_client.close()
    except Exception as e:
        print(f"Failed to list topics: {e}")


def delete_all_topics(bootstrap_servers='localhost:9092'):
    """
    Delete all OKX topics (use with caution!).
    This is useful for resetting the environment.
    """
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        existing_topics = admin_client.list_topics()
        okx_topics = [t for t in existing_topics if t.startswith('okx-')]
        
        if not okx_topics:
            print("No OKX topics found")
            return
        
        print(f"Deleting {len(okx_topics)} topics...")
        admin_client.delete_topics(topics=okx_topics)
        
        for topic in okx_topics:
            print(f"✅ Deleted: {topic}")
        
        admin_client.close()
        print("\nAll OKX topics deleted successfully")
        
    except Exception as e:
        print(f"❌ Failed to delete topics: {e}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Setup Redpanda topics for OKX data ingestion')
    parser.add_argument('--bootstrap-servers', default='localhost:19092', 
                        help='Redpanda bootstrap servers (default: localhost:19092)')
    parser.add_argument('--delete', action='store_true',
                        help='Delete all OKX topics instead of creating them')
    
    args = parser.parse_args()
    
    if args.delete:
        confirm = input("⚠️  Are you sure you want to delete all OKX topics? (yes/no): ")
        if confirm.lower() == 'yes':
            delete_all_topics(args.bootstrap_servers)
        else:
            print("Aborted")
    else:
        create_topics(args.bootstrap_servers)
