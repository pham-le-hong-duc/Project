import duckdb
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from plugins.spark_config import S3_BUCKET, DUCKDB_PATH

def parse_interval_to_minutes(interval_str):
    unit = interval_str[-1].lower()
    try:
        value = int(interval_str[:-1])
    except:
        return 60 # Default fallback

    if unit == 'm': return value
    if unit == 'h': return value * 60
    if unit == 'd': return value * 1440
    return 60
def get_last_processed_time(interval_name):
    con = duckdb.connect(DUCKDB_PATH)
    try:
        # Kiểm tra bảng có tồn tại không
        table_exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'fact_market_features'").fetchone()[0]
        if table_exists == 0:
            return None

        query = f"SELECT MAX(candle_time) FROM fact_market_features WHERE interval = '{interval_name}'"
        result = con.execute(query).fetchone()[0]
        return result # Trả về datetime hoặc None
    except:
        return None
    finally:
        con.close()
def calculate_features_incremental(spark, interval_name, interval_window):
    print(f"\n🚀 Processing Features: {interval_name} ({interval_window})")
    # 1. TÍNH TOÁN BUFFER DYNAMIC (Quan trọng)
    interval_minutes = parse_interval_to_minutes(interval_name)

    # Quy tắc an toàn: Lùi lại ít nhất 2 lần độ dài nến + 10 phút trễ
    # Ví dụ: 12h -> Lùi 24h. 1m -> Lùi 12 phút.
    buffer_minutes = (interval_minutes * 2) + 10

    last_time = get_last_processed_time(interval_name)
    cutoff_time = datetime.now() - timedelta(minutes=1)
    input_path = f"s3a://{S3_BUCKET}/silver/trades/"

    # Đọc dữ liệu (Tối ưu: Chỉ đọc các file parquet cần thiết nếu có partition date)
    # Ở đây đọc full folder rồi filter (Spark sẽ tự tối ưu đẩy filter xuống)
    df = spark.read.parquet(input_path)

    # if last_time:
    #     start_filter = last_time - timedelta(minutes=buffer_minutes)
    #     print(f"   ℹ️ Incremental Mode: Reading trades after {start_filter} (Buffer: {buffer_minutes}m)")
    #     df = df.filter(F.col("trade_time") >= F.lit(start_filter))
    # else:
    #     print("   ℹ️ Full Load Mode: Reading all trades")
    if last_time:
        # Thời điểm bắt đầu cần lấy dữ liệu (trừ hao buffer)
        start_timestamp = last_time - timedelta(minutes=buffer_minutes)

        # --- KỸ THUẬT PARTITION PRUNING (QUAN TRỌNG) ---
        # Chuyển đổi timestamp thành chuỗi ngày (YYYY-MM-DD)
        start_date_str = start_timestamp.strftime("%Y-%m-%d")

        print(f"   ℹ️ Incremental Mode: Reading from {start_timestamp}")
        print(f"   📂 Partition Pruning: Reading folders from date_part >= {start_date_str}")

        # Bước 1: Lọc Folder (Nhanh) - Spark sẽ bỏ qua các folder cũ
        # date_part vẫn có trong file parquet dược đưa lên silver nhưng sẽ ko insert vào table DB
        df = df.filter(F.col("date_part") >= F.lit(start_date_str))

        # Bước 2: Lọc chi tiết Time (Chính xác) - Lấy đúng phút/giây
        df = df.filter(F.col("trade_time") >= F.lit(start_timestamp))

    else:
        print("   ℹ️ Full Load Mode: Reading all partitions")
    try:
        # Lấy dòng có thời gian nhỏ nhất trong batch hiện tại
        min_row = df.select(F.min("trade_time")).collect()
        min_data_time = min_row[0][0]

        if min_data_time is None:
            print("   ⚠️ No data found in this range.")
            return False

        print(f"   🕒 Batch Min Data Time: {min_data_time}")
    except Exception as e:
        print(f"   ⚠️ Error checking min time: {e}")
        return False
    # 2. Tính toán Features (Giữ nguyên logic phức tạp của bạn)
    df = df.withColumn("turnover", F.col("price") * F.col("quantity"))

    features_df = df.groupBy(
        F.col("symbol"),
        F.window(F.col("trade_time"), interval_window).alias("window")
    ).agg(
        # Basic
        F.min(F.struct("trade_time", "price")).getItem("price").alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.max(F.struct("trade_time", "price")).getItem("price").alias("close"),
        F.sum("quantity").alias("volume"),
        F.sum("turnover").alias("total_turnover"),
        F.count("*").alias("trade_count"),
        # Advanced (Skew, Kurtosis...)
        F.stddev("price").alias("price_std"),
        F.skewness("price").alias("price_skew"),
        F.kurtosis("price").alias("price_kurtosis"),
        # percentile_approx là hàm xấp xỉ rất nhanh trên Big Data
        F.percentile_approx("price", 0.25).alias("price_q25"),
        F.percentile_approx("price", 0.50).alias("price_median"),
        F.percentile_approx("price", 0.75).alias("price_q75"),
        # Buy/Sell Volume
        F.sum(F.when(F.col("side") == "buy", F.col("quantity")).otherwise(0)).alias("vol_buy"),
        F.sum(F.when(F.col("side") == "sell", F.col("quantity")).otherwise(0)).alias("vol_sell"),
        F.count(F.when(F.col("side") == "buy", 1)).alias("count_buy"),
        F.count(F.when(F.col("side") == "sell", 1)).alias("count_sell"),
        # --- D. SIZE DISTRIBUTION (Phân phối khối lượng lệnh) ---
        F.max("quantity").alias("size_max"),
        F.avg("quantity").alias("size_mean"),
        F.stddev("quantity").alias("size_std")
    )
    features_df = features_df.filter(
        (F.col("window.end") <= F.lit(cutoff_time)) &
        (F.col("window.start") >= F.lit(min_data_time))
    )

    # Kiểm tra xem có dữ liệu không sau khi lọc
    if features_df.rdd.isEmpty():
        print(f"   ⚠️ No closed candles found for {interval_name}. Waiting for more data...")
        return False
    final_df = features_df.select(
        F.col("symbol"),
        F.col("window.start").alias("candle_time"),
        F.lit(interval_name).alias("interval"),
        "open", "high", "low", "close", "volume", "trade_count",
        "price_std","price_skew", "price_kurtosis", "price_q25","price_median","price_q75",
        "vol_buy", "vol_sell","count_buy","count_sell","size_max","size_mean","size_std",
        (F.col("total_turnover") / F.col("volume")).alias("vwap"),
        F.current_timestamp().alias("ingestion_time")
    )

    # 3. Ghi ra Staging Path (Chế độ OVERWRITE cho folder staging này thôi)
    # Folder này chỉ chứa data của lần chạy này, không chứa data cũ
    # Partition by date_part để tối ưu file size
    final_df = final_df.withColumn("date_part", F.date_format("candle_time", "yyyy-MM-dd"))

    # Path riêng cho interval này trong staging
    staging_path = f"s3a://{S3_BUCKET}/silver/agg_trades/{interval_name}"

    print(f"   💾 Writing to Staging: {staging_path}")
    final_df.write.mode("overwrite").parquet(staging_path)

    return True
def merge_to_duckdb(interval_name):
    print(f"🦆 Merging {interval_name} into DuckDB...")
    con = duckdb.connect(DUCKDB_PATH)
    with open('/opt/airflow/SQL_db/config_dw/warehouse_source.sql', 'r') as f:
        sql_script = f.read()
    con.execute(sql_script)
    staging_source = f"s3://{S3_BUCKET}/silver/agg_trades/{interval_name}/*.parquet"

    try:
        # Create Table (Nếu chưa có)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS fact_market_features AS
            SELECT * FROM read_parquet('{staging_source}') LIMIT 0
        """)

        # Xóa dữ liệu cũ trùng lặp (Dựa trên symbol, time VÀ interval)
        # Vì ta đang chạy loop, chỉ xóa những dòng thuộc interval đang xử lý
        print("   🔄 Cleaning overlapping data...")
        con.execute(f"""
            DELETE FROM fact_market_features
            WHERE interval = '{interval_name}'
            AND (symbol, candle_time) IN (
                SELECT symbol, candle_time FROM read_parquet('{staging_source}')
            )
        """)

        # Insert mới
        print("   📥 Inserting new data...")
        con.execute(f"""
            INSERT INTO fact_market_features
            SELECT * FROM read_parquet('{staging_source}')
        """)

        print("   ✅ Merge Complete.")

    except Exception as e:
        print(f"   ⚠️ DuckDB Merge Error: {e}")
    finally:
        con.close()
def agg_trades(spark):
    # Danh sách Interval cần chạy
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "parallel_pool")
    intervals = [
        ("1m", "1 minute"),
        ("5m", "5 minutes"),
        ("15m", "15 minutes"),
        ("1h", "1 hour"),
        ("4h", "4 hours"),
        ("12h", "12 hours") # Test interval lớn
    ]

    print(f"STARTING BATCH PIPELINE FOR {len(intervals)} INTERVALS...")

    for name, window in intervals:
        # Bước 1: Tính toán
        calculate_features_incremental(spark, name, window)
        # Bước 2: Nạp vào Fact
        merge_to_duckdb(name)
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)
#agg_trades()