import duckdb
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, max, min, sum, struct, lit, date_format, current_timestamp

# --- CONFIG ---
S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS = "minio"
S3_SECRET = "minio123"
S3_BUCKET = "trading-okx"

# Đường dẫn Checkpoint (BẮT BUỘC PHẢI CÓ để resume)
# Spark sẽ lưu trạng thái vào đây. Tuyệt đối không xóa thư mục này nếu muốn chạy tiếp.
CHECKPOINT_ROOT = f"s3a://{S3_BUCKET}/checkpoints/ohlc_parquet_v1/"
DUCKDB_PATH = '/mnt/d/learn/DE/Semina_project/datawarehouse.duckdb'
# Cấu hình độ trễ cho phép (Watermark)
# Dữ liệu đến muộn quá 10 phút sẽ bị bỏ qua, nến sẽ đóng sau 10 phút.
WATERMARK_DELAY = "10 minutes"
def get_spark():
    return SparkSession.builder \
        .appName("OKX_Bronze_To_Silver_trade") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
def get_duckdb_conn():
    con = duckdb.connect(DUCKDB_PATH)
    with open('/mnt/d/learn/DE/Semina_project/SQL_db/config_dw/warehouse_source.sql', 'r') as f:
             sql_script = f.read()
    con.execute(sql_script)
    return con
def load_to_duckdb(interval_name):
    """
    Hàm này chạy SAU KHI Spark đã ghi xong file Parquet.
    Nó ra lệnh cho DuckDB đọc file Parquet mới và insert vào bảng.
    """
    print(f"🦆 [DuckDB] Đang nạp dữ liệu OHLC ({interval_name}) vào Warehouse...")
    con = get_duckdb_conn()

    # Spark partition theo: interval=1h/date_part=2023-12-01
    parquet_source = f"s3://{S3_BUCKET}/silver/calculated_ohlc/interval={interval_name}/*/*.parquet"

    try:
        # 2.1 Load vào Staging (Incremental)
        # Bảng staging: ohlc
        # Lưu ý: Interval là hardcode từ tham số hàm vì Spark partition theo folder này
        con.execute(f"""
            INSERT INTO ohlc (symbol, candle_time, open, high, low, close, volume, interval)
            SELECT symbol, candle_time, open, high, low, close, volume, '{interval_name}'
            FROM read_parquet('{parquet_source}', hive_partitioning=1)
            WHERE candle_time > (SELECT COALESCE(MAX(candle_time), '1970-01-01'::TIMESTAMP) FROM ohlc WHERE interval = '{interval_name}')
        """)
        print("   ✅ Staging Loaded.")

        # 2.2 Update Dim Time
        # Tạo ngày mới nếu chưa có
        con.execute("""
            INSERT INTO dim_time
            SELECT DISTINCT
                CAST(strftime(candle_time, '%Y%m%d') AS INTEGER) as date_key,
                CAST(candle_time AS DATE),
                EXTRACT(YEAR FROM candle_time), EXTRACT(QUARTER FROM candle_time),
                EXTRACT(MONTH FROM candle_time), EXTRACT(DAY FROM candle_time),
                ISODOW(candle_time), CASE WHEN ISODOW(candle_time) IN (6, 7) THEN TRUE ELSE FALSE END
            FROM ohlc
            WHERE CAST(strftime(candle_time, '%Y%m%d') AS INTEGER) NOT IN (SELECT date_key FROM dim_time)
        """)

        # 2.3 Insert Fact Table (fact_ohlc_calculated)
        # JOIN với dim_symbol và dim_time để lấy Key chuẩn
        con.execute(f"""
            INSERT INTO fact_ohlc_calculated (symbol_key, date_key, interval, candle_time, open, high, low, close, volume)
            SELECT
                d.symbol_key,
                CAST(strftime(s.candle_time, '%Y%m%d') AS INTEGER) as date_key,
                s.interval,
                s.candle_time,
                s.open, s.high, s.low, s.close, s.volume
            FROM ohlc s
            JOIN dim_symbol d ON s.symbol = d.symbol_code
            WHERE s.candle_time > (SELECT COALESCE(MAX(candle_time), '1970-01-01'::TIMESTAMP) FROM fact_ohlc_calculated WHERE interval = '{interval_name}')
            AND s.interval = '{interval_name}'
        """)
        print("   ✅ Gold (Fact) Loaded.")

    except Exception as e:
        print(f"   ⚠️ DuckDB Load Error: {e}")
        # Không raise để pipeline chạy tiếp interval khác
    finally:
        con.close()

def run_streaming_ohlc(spark, interval_name="1m", interval_window="1 minute"):
    print(f"🚀 Đang xử lý khung thời gian: {interval_name}")

    # 1. INPUT: READ STREAM (Chỉ đọc file mới)
    # Spark tự theo dõi file nào mới trong thư mục này
    input_path = f"s3a://{S3_BUCKET}/silver/trades/"

    # Lấy schema từ 1 file mẫu (để tránh lỗi schema evolution)
    try:
        schema = spark.read.parquet(input_path).schema
    except:
        print("⚠️ Chưa có data trades. Thoát.")
        return

    df_trades = spark.readStream \
        .schema(schema) \
        .format("parquet") \
        .option("maxFilesPerTrigger", 1000) \
        .load(input_path)

    # 2. TRANSFORM: AGGREGATE VỚI WATERMARK
    # Bắt buộc phải có withWatermark để dùng mode 'append'

    df_ohlc = df_trades \
        .withWatermark("trade_time", WATERMARK_DELAY) \
        .groupBy(
            col("symbol"),
            window(col("trade_time"), interval_window).alias("window_time")
        ).agg(
            min(struct(col("trade_time"), col("price"))).getItem("price").alias("open"),
            max("price").alias("high"),
            min("price").alias("low"),
            max(struct(col("trade_time"), col("price"))).getItem("price").alias("close"),
            sum("quantity").alias("volume")
        )

    # Chuẩn hóa đầu ra
    df_final = df_ohlc.select(
        col("symbol"),
        col("window_time.start").alias("candle_time"),
        col("open"), col("high"), col("low"), col("close"), col("volume"),
        lit(interval_name).alias("interval"),
        date_format(col("window_time.start"), "yyyy-MM-dd").alias("date_part")
    )

    # 3. OUTPUT: WRITE STREAM (APPEND ONLY)
    # Output path riêng cho từng interval
    output_path = f"s3a://{S3_BUCKET}/silver/calculated_ohlc/"
    checkpoint_path = f"{CHECKPOINT_ROOT}/{interval_name}"
    # Checkpoint riêng cho từng interval (Quan trọng!)

    query = df_final.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(availableNow=True) \
        .partitionBy("interval", "date_part") \
        .start(output_path)

    # trigger(availableNow=True):
    # - Đọc hết data mới -> Tính toán -> Ghi xuống Parquet -> Lưu Checkpoint -> Stop.
    # - Không treo máy chờ data như streaming thông thường.

    query.awaitTermination()

    print(f"✅ Hoàn tất xử lý {interval_name}. Data đã được Append vào MinIO.")
    load_to_duckdb(interval_name)

def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # Bạn có thể chạy nhiều interval
    intervals = [
        ("1m", "1 minute"),
        ("5m", "5 minute")
    ]

    for name, window_duration in intervals:
        run_streaming_ohlc(spark, name, window_duration)

    spark.stop()
main()