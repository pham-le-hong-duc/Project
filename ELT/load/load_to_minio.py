# import json
# import gzip
# import threading
# from datetime import datetime
# from confluent_kafka import Consumer
# import boto3
# import logging
# # --- CONFIG LOGGING ---
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s | %(levelname)s | %(message)s',
#     datefmt='%Y-%m-%d %H:%M:%S'
# )
# logger = logging.getLogger("airflow.task")
#
# # --- CONFIG APP ---
# KAFKA_BOOTSTRAP = "redpanda:29092"
# TOPICS = ["okx_trades", "okx_funding", "okx_orderbook", "okx_ohlc_mark", "okx_ohlc_index"]
# S3_ENDPOINT = "http://minio:9000"
# S3_ACCESS = "minio"
# S3_SECRET = "minio123"
# S3_BUCKET = "trading-okx"
# FLUSH_INTERVAL = 10
# MAX_BUFFER_BYTES = 1 * 1024  # 200KB (Cấu hình thực tế hợp lý hơn 1KB)
#
# # --- SETUP S3 & KAFKA ---
# s3 = boto3.client(
#     "s3",
#     endpoint_url=S3_ENDPOINT,
#     aws_access_key_id=S3_ACCESS,
#     aws_secret_access_key=S3_SECRET,
#     region_name="us-east-1"
# )
#
#
#
# buffers = {}
# buf_lock = threading.Lock()
# stop_event = threading.Event()
#
#
# # --- FUNCTIONS ---
#
# def s3_put_bytes(file_key, data_bytes):
#     """Upload data lên MinIO"""
#     try:
#         try:
#             s3.head_bucket(Bucket=S3_BUCKET)
#         except Exception:
#             logger.warning(f"Bucket {S3_BUCKET} not found, creating...")
#             s3.create_bucket(Bucket=S3_BUCKET)
#
#         # Upload
#         s3.put_object(Bucket=S3_BUCKET, Key=file_key, Body=data_bytes)
#         logger.info(f"Uploaded: {file_key} ({len(data_bytes)} bytes)")
#         return True
#     except Exception as e:
#         logger.error(f"Upload ERROR: {e}")
#         return False
# def flush_key(k):
#     """Lấy data từ buffer, nén Gzip và upload (Dùng cho Scheduled Flush)"""
#     with buf_lock:
#         if k not in buffers or len(buffers[k]) == 0:
#             return
#
#         # Lấy data và xóa buffer ngay lập tức
#         data = bytes(buffers[k])
#         buffers[k] = bytearray()
#
#     try:
#         topic, dh = k.split("|")
#         date, hour = dh.rsplit("-", 1)
#         ts = datetime.now().strftime("%Y%m%d_%H%M%S")
#
#         object_key = f"bronze/{topic}/{date}/{hour}/{topic}_{ts}.jsonl.gz"
#
#         # --- FIX QUAN TRỌNG: NÉN GZIP TRƯỚC KHI UPLOAD ---
#         compressed_data = gzip.compress(data)
#
#         s3_put_bytes(object_key, compressed_data)
#
#     except Exception as e:
#         logger.error(f"Error flushing key {k}: {e}")
# def flush_thread_func():
#     """Thread chạy ngầm định kỳ"""
#     logger.info("⏳ Flush thread started")
#     while not stop_event.is_set():
#         if stop_event.wait(FLUSH_INTERVAL):
#             break
#
#         with buf_lock:
#             keys = list(buffers.keys())
#
#         for k in keys:
#             if len(buffers[k]) > 0:
#                 flush_key(k)
#     logger.info("⏳ Flush thread stopped.")
# def flush_all_remaining():
#     logger.info("💾 Flushing ALL remaining data...")
#     with buf_lock:
#         keys = list(buffers.keys())
#
#     count = 0
#     for k in keys:
#         if len(buffers[k]) > 0:
#             flush_key(k)
#             count += 1
#     logger.info(f"🏁 Final flush completed. Processed {count} keys.")
# def add_record(topic, record):
#     ts = record.get("received_at")
#     try:
#         dt = datetime.fromisoformat(ts) if ts else datetime.now()
#     except:
#         dt = datetime.now()
#
#     date = dt.strftime("%Y-%m-%d")
#     hour = dt.strftime("%H")
#     key = f"{topic}|{date}-{hour}"
#
#     # Chuẩn bị dòng JSONL
#     line = json.dumps(record) + "\n"
#
#     # Lưu ý: Ta lưu text (bytes) vào buffer, chỉ nén khi upload
#     line_bytes = line.encode('utf-8')
#
#     with buf_lock:
#         if key not in buffers:
#             buffers[key] = bytearray()
#         buffers[key].extend(line_bytes)
#
#         current_len = len(buffers[key])
#
#         # FLUSH NGAY LẬP TỨC NẾU ĐẦY
#         if current_len >= MAX_BUFFER_BYTES:
#             logger.info(f"⚡ Buffer full for {key} ({current_len} bytes), flushing...")
#
#             # Copy data ra và clear buffer
#             data_to_upload = bytes(buffers[key])
#             buffers[key] = bytearray()
#
#             # Tạo đường dẫn (Logic giống hệt flush_key)
#             filepath = f"bronze/{topic}/{date}/{hour}/{topic}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl.gz"
#
#             # Chạy thread riêng để upload (đã fix nén Gzip)
#             threading.Thread(
#                 target=lambda: s3_put_bytes(
#                     filepath,
#                     gzip.compress(data_to_upload)  # <--- ĐÃ CÓ GZIP Ở ĐÂY (Code cũ của bạn đúng chỗ này)
#                 ),
#                 daemon=True
#             ).start()
# def load_data_to_minio():
#     c = Consumer({
#         "bootstrap.servers": KAFKA_BOOTSTRAP,
#         "group.id": "trade-bronze",  # Đổi group ID mới để đọc lại từ đầu cho chắc
#         "auto.offset.reset": "earliest"
#     })
#     c.subscribe(TOPICS)
#     t = threading.Thread(target=flush_thread_func)
#     t.start()
#
#     print(f"🚀 Consumer started. Listening on {KAFKA_BOOTSTRAP}")
#     try:
#         while True:
#             msg = c.poll(1.0)
#             if msg is None:
#                 continue
#             if msg.error():
#                 logger.error(f"Kafka error: {msg.error()}")
#                 continue
#
#             try:
#                 val = json.loads(msg.value().decode())
#                 add_record(msg.topic(), val)
#                 # print(".", end="", flush=True) # Uncomment nếu muốn xem dot log
#             except Exception as e:
#                 logger.error(f"Decode error: {e}")
#
#     except KeyboardInterrupt:
#         logger.info("\n🛑 User stopped consumer...")
#     finally:
#         stop_event.set()
#         t.join()
#         flush_all_remaining()
#         c.close()
#         logger.info("👋 Consumer stopped gracefully.")
# #load_data_to_miniov()
import json
import gzip
import threading
import time
import logging
from datetime import datetime
from confluent_kafka import Consumer
import boto3

# --- CONFIG LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("airflow.task")

# --- GLOBAL VARIABLES ---
# Chỉ giữ lại các biến cấu trúc dữ liệu, KHÔNG khởi tạo connection ở đây
buffers = {}
buf_lock = threading.Lock()
stop_event = threading.Event()
s3_client = None  # Sẽ khởi tạo bên trong hàm

# --- CONSTANTS DEFAULT ---
# Các giá trị mặc định nếu DAG không truyền vào
DEFAULT_TOPICS = ["okx_trades", "okx_funding", "okx_orderbook", "okx_ohlc_mark", "okx_ohlc_index"]
DEFAULT_BUCKET = "trading-okx"


# --- FUNCTIONS ---

def get_s3_client(endpoint, access_key, secret_key):
    """Tạo kết nối S3 an toàn bên trong hàm"""
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1"
    )


def s3_put_bytes(file_key, data_bytes, bucket_name):
    """Upload data lên MinIO dùng client toàn cục đã init"""
    global s3_client
    if not s3_client:
        logger.error("❌ S3 Client chưa được khởi tạo!")
        return False

    try:
        # Kiểm tra bucket tồn tại (cache 1 lần để tối ưu)
        if not hasattr(s3_put_bytes, "bucket_checked"):
            try:
                s3_client.head_bucket(Bucket=bucket_name)
            except Exception:
                logger.warning(f"⚠️ Bucket {bucket_name} chưa có, đang tạo...")
                s3_client.create_bucket(Bucket=bucket_name)
            s3_put_bytes.bucket_checked = True

        # Upload
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=data_bytes)
        logger.info(f"✅ Uploaded: {file_key} ({len(data_bytes)} bytes)")
        return True
    except Exception as e:
        logger.error(f"❌ Upload ERROR: {e}")
        return False


def flush_key(k, bucket_name):
    """Lấy data từ buffer, nén Gzip và upload"""
    with buf_lock:
        if k not in buffers or len(buffers[k]) == 0:
            return
        data = bytes(buffers[k])
        buffers[k] = bytearray()  # Xóa buffer ngay

    try:
        topic, dh = k.split("|")
        date, hour = dh.rsplit("-", 1)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Đường dẫn: bronze/topic/date/hour/filename
        object_key = f"bronze/{topic}/{date}/{hour}/{topic}_{ts}.jsonl.gz"

        # Nén Gzip
        compressed_data = gzip.compress(data)

        # Gọi hàm upload
        s3_put_bytes(object_key, compressed_data, bucket_name)

    except Exception as e:
        logger.error(f"Error flushing key {k}: {e}")


def flush_thread_func(interval, bucket_name):
    """Thread chạy ngầm định kỳ flush data"""
    logger.info("⏳ Flush thread started")
    while not stop_event.is_set():
        if stop_event.wait(interval):
            break

        # Snapshot keys
        with buf_lock:
            keys = list(buffers.keys())

        for k in keys:
            with buf_lock:
                has_data = len(buffers[k]) > 0

            if has_data:
                flush_key(k, bucket_name)

    logger.info("⏳ Flush thread stopped.")


def flush_all_remaining(bucket_name):
    logger.info("💾 Flushing ALL remaining data...")
    with buf_lock:
        keys = list(buffers.keys())
    for k in keys:
        flush_key(k, bucket_name)


def add_record(topic, record, max_bytes, bucket_name):
    ts = record.get("received_at")
    try:
        dt = datetime.fromisoformat(ts) if ts else datetime.now()
    except:
        dt = datetime.now()

    date = dt.strftime("%Y-%m-%d")
    hour = dt.strftime("%H")
    key = f"{topic}|{date}-{hour}"

    line = json.dumps(record) + "\n"
    line_bytes = line.encode('utf-8')

    with buf_lock:
        if key not in buffers:
            buffers[key] = bytearray()
        buffers[key].extend(line_bytes)
        current_len = len(buffers[key])

    # Flush ngay nếu đầy buffer
    if current_len >= max_bytes:
        logger.info(f"⚡ Buffer full for {key} ({current_len} bytes), flushing...")
        flush_key(key, bucket_name)


# --- MAIN ENTRY POINT CHO AIRFLOW ---
def load_data_to_minio(
        kafka_bootstrap="redpanda:29092",
        s3_endpoint="http://minio:9000",
        s3_access="minio",
        s3_secret="minio123",
        s3_bucket=DEFAULT_BUCKET,
        runtime_seconds=3500  # Default 58 phút
):
    """
    Hàm chính:
    1. Nhận tham số từ DAG (quan trọng!).
    2. Chạy có thời hạn (runtime_seconds) rồi tự dừng.
    """
    global s3_client

    # 1. Khởi tạo S3 Client (Quan trọng: Init bên trong hàm)
    s3_client = get_s3_client(s3_endpoint, s3_access, s3_secret)

    logger.info(f"🚀 Starting Loader Task.")
    logger.info(f"   Kafka: {kafka_bootstrap}")
    logger.info(f"   MinIO: {s3_endpoint}")
    logger.info(f"   Bucket: {s3_bucket}")
    logger.info(f"   Duration: {runtime_seconds}s")

    # 2. Cấu hình Consumer
    c = Consumer({
        "bootstrap.servers": kafka_bootstrap,
        "group.id": "airflow-minio-loader",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True
    })

    try:
        c.subscribe(DEFAULT_TOPICS)
        logger.info(f"🎧 Subscribed to: {DEFAULT_TOPICS}")
    except Exception as e:
        logger.error(f"❌ Failed to subscribe: {e}")
        return

    # 3. Start Flush Thread
    stop_event.clear()
    # Truyền tham số bucket vào thread
    t = threading.Thread(target=flush_thread_func, args=(10, s3_bucket))
    t.start()

    start_time = time.time()
    msg_count = 0

    try:
        # --- LOGIC THỜI GIAN: Chạy X giây rồi dừng ---
        while (time.time() - start_time) < runtime_seconds:
            msg = c.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                val = json.loads(msg.value().decode())
                add_record(msg.topic(), val, 1024 * 1024, s3_bucket)  # 1MB buffer limit
                msg_count += 1

                if msg_count % 5000 == 0:
                    logger.info(f"Processed {msg_count} messages...")
            except Exception as e:
                logger.error(f"Decode error: {e}")

        logger.info(f"⏰ Time limit reached ({runtime_seconds}s). Stopping task...")

    except KeyboardInterrupt:
        logger.info("🛑 Stopped manually.")
    except Exception as e:
        logger.error(f"Critical Error: {e}")
        raise e
    finally:
        # Dọn dẹp
        stop_event.set()
        t.join()
        flush_all_remaining(s3_bucket)
        c.close()
        logger.info("👋 Loader stopped gracefully.")


# Nếu muốn test thủ công bằng lệnh python load_to_minio.py
if __name__ == "__main__":
    load_data_to_minio(runtime_seconds=60)  # Chạy thử 60s rồi dừng