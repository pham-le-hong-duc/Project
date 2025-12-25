# create_topics.py
from confluent_kafka.admin import AdminClient, NewTopic
import logging
KAFKA_BOOTSTRAP = "localhost:9092"
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

topics = [
    "okx_trades",
    "okx_orderbook",
    "okx_funding",
    "okx_ohlc_mark",
    "okx_ohlc_index",
    "orther" #fallback
]

n_partition = 3 #paralel 3 throughput
replicate = 1  # Vì chạy Local 1 node Redpanda thì replica chỉ là 1. Nếu chạy Cloud thì để 3.

# Cấu hình bổ sung (Optional)
# retention.ms: Thời gian giữ data (ví dụ 1 ngày = 86400000 ms) để đỡ tốn ổ cứng local

TOPIC_CONFIG = {
    "retention.ms": "86400000",
    "cleanup.policy": "delete"
}


def create_kafka_topics():
    admin_client = AdminClient({
        "bootstrap.servers": KAFKA_BOOTSTRAP
    })
    #try:
    cluster_metadata = admin_client.list_topics(timeout=10)
    existing_topics = set(t for t in cluster_metadata.topics)
    # except Exception as e:
    #     logger.error(f"Failed to list topics: {e}")
    #     return
    new_topics = []
    for topic_name in topics:
        if topic_name not in existing_topics:
            logger.info(f"check exists topic: {topic_name}")
            new_topics.append(
                NewTopic(
                    topic=topic_name,
                    num_partitions=n_partition,
                    replication_factor=replicate,
                    config=TOPIC_CONFIG
                )
            )
        else:
            logger.warning(f"topic '{topic_name}' exists")
    if not new_topics:
        logger.info("no new topics to create.")
        return
    # create_topics trả về dict: {topic_name: future}
    futures = admin_client.create_topics(new_topics)
    for topic, future in futures.items():
        try:
            future.result()  # Block chờ kết quả
            logger.info(f"✅ success created topic {topic}")
        except Exception as e:
            logger.error(f"❌ failed to create topic {topic}: {e}")


if __name__ == "__main__":
    create_kafka_topics()