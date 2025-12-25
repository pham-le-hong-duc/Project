import duckdb
from datetime import timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from functools import reduce
from plugins.spark_config import S3_BUCKET, DUCKDB_PATH

PATH_SILVER_BASE = f"s3a://trading-okx/silver"
#SYMBOL= "btc-usdt-swap"
SYMBOL= "btc-usdt"
PATH_GOLD_STAGING = f"s3a://{S3_BUCKET}/gold/staging_merged_features"
PATH_GOLD_FINAL = f"s3a://{S3_BUCKET}/gold/fact_merged_features"
SOURCE_MAPPING = {
    "trade": "agg_trades",
    "book": "agg_orderbook",
    "index": "indexPriceKlines",
    "mark": "markPriceKlines"
}
INTERVALS = ["1m","5m", "15m", "1h", "4h", "1d"]
# --- 1. UTILS ---
def get_last_processed_time(interval):
    """Lấy timestamp cuối cùng từ DuckDB để chạy Incremental"""
    con = duckdb.connect(DUCKDB_PATH)
    try:
        # Load MinIO config
        with open('/opt/airflow/SQL_db/config_dw/warehouse_source.sql', 'r') as f:
            con.execute(f.read())

        # Check table existence
        exists = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'fact_merged_features'"
        ).fetchone()[0]

        if exists == 0: return None

        # Lấy max time của interval tương ứng
        query = f"SELECT MAX(timestamp) FROM fact_merged_features WHERE interval = '{interval}'"
        result = con.execute(query).fetchone()[0]
        return result
    except Exception as e:
        print(f"⚠️ DuckDB Info: {e}")
        return None
    finally:
        con.close()
def load_agg_source(spark, source_alias, table_name, interval, last_time):
    path = f"{PATH_SILVER_BASE}/{table_name}/{interval}/"
    try:
        df = spark.read.parquet(path + "*")
    except Exception:
        # print(f"   ⚠️ Path not found: {path}") # Bớt log rác
        return None

    if last_time:
        buffer_time = last_time - timedelta(hours=1)
        start_date_str = buffer_time.strftime("%Y-%m-%d")

        # Pruning Partition
        if "date_part" in df.columns:
            df = df.filter(F.col("date_part") >= F.lit(start_date_str))

        # Filter Time
        time_col = "candle_time" if "candle_time" in df.columns else "close_time"
        if time_col in df.columns:
            df = df.filter(F.col(time_col) >= F.lit(buffer_time))

    if df.rdd.isEmpty(): return None

    # Standardization
    for t_col in ["close_time", "window_end", "candle_time"]:
        if t_col in df.columns:
            df = df.withColumnRenamed(t_col, "timestamp")
            break

    df = df.withColumn("timestamp", F.col("timestamp").cast(TimestampType()))

    # Renaming & Cleanup
    exclude_cols = ["timestamp", "symbol", "interval", "date_part", "ingestion_time"]
    rename_mapping = {}
    for col_name in df.columns:
        if col_name not in exclude_cols:
            rename_mapping[col_name] = f"{source_alias}_{col_name}"

    for old, new in rename_mapping.items():
        df = df.withColumnRenamed(old, new)

    # Drop metadata columns của Silver để tránh trùng khi Union/Join
    for col in ["symbol", "interval", "date_part", "ingestion_time"]:
        if col in df.columns: df = df.drop(col)

    return df
def load_funding_rate(spark, last_time):
    # Đường dẫn funding rate
    path = f"s3a://{S3_BUCKET}/silver/funding_rate/*/*"
    try:
        df = spark.read.parquet(path)
    except:
        print("   ⚠️ No Funding Rate data found")
        return None

    if last_time:
        # Buffer lớn hơn cho funding vì nó thưa (8h/lần)
        buffer_funding = last_time - timedelta(days=1)
        # funding_time đã là Timestamp, so sánh trực tiếp được
        df = df.filter(F.col("funding_time") >= F.lit(buffer_funding))

    df = df.withColumnRenamed("funding_time", "timestamp")

    # Đảm bảo kiểu dữ liệu là TimestampType (cho chắc chắn)
    df = df.withColumn("timestamp", F.col("timestamp").cast(TimestampType()))

    # Chỉ lấy cột cần thiết
    return df.select("timestamp", F.col("funding_rate").alias("funding_rate"))
def merge_features_for_interval(spark, interval):
    print(f"\n🚀 Processing Interval: {interval}")
    last_time = get_last_processed_time(interval)
    data_frames = []

    # 1. Load Sources Agg (Loop)
    for alias, table_name in SOURCE_MAPPING.items():
        print(f"   📂 Loading {table_name}...")
        df = load_agg_source(spark, alias, table_name, interval, last_time)
        if df is not None:
            data_frames.append(df)

    if not data_frames:
        print(f"   ⚠️ No agg data found for {interval}. Skipping.")
        # --- SỬA TẠI ĐÂY ---
        # Cũ: return False  <-- Nguyên nhân gây lỗi
        # Mới: return None
        return None

    # 3. Merge Agg Data trước
    print(f"   🔗 Merging {len(data_frames)} agg sources...")

    # Hàm merge tối ưu (bỏ .count())
    def join_dfs(df1, df2):
        return df1.join(df2, on="timestamp", how="full_outer") \
                  .withColumn("timestamp", F.coalesce(df1["timestamp"], df2["timestamp"]))

    merged_df = reduce(join_dfs, data_frames)

    # 5. Clean & Enrich
    merged_df = merged_df \
        .withColumn("interval", F.lit(interval)) \
        .withColumn("symbol", F.lit(SYMBOL)) \
        .withColumn("date_part", F.date_format("timestamp", "yyyy-MM-dd")) \
        .withColumn("processed_at", F.current_timestamp())

    return merged_df
def sync_unified_to_duckdb(staging_path):
    print("   🦆 Syncing Unified Data to DuckDB...")
    con = duckdb.connect(DUCKDB_PATH)

    # DuckDB đọc S3 dùng s3://
    duck_read_path = staging_path.replace("s3a://", "s3://") + "/*/*.parquet"

    try:
        with open('/opt/airflow/SQL_db/config_dw/warehouse_source.sql', 'r') as f:
            con.execute(f.read())

        # 1. Tạo bảng (Schema Evolution)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS fact_merged_features AS
            SELECT * FROM read_parquet('{duck_read_path}', hive_partitioning=1) LIMIT 0
        """)

        # 2. Xóa dữ liệu cũ (Dựa trên cặp interval + timestamp có trong file mới)
        print("   🔄 Cleaning overlaps...")
        con.execute(f"""
            DELETE FROM fact_merged_features
            WHERE (interval, timestamp) IN (
                SELECT interval, timestamp 
                FROM read_parquet('{duck_read_path}', hive_partitioning=1)
            )
        """)

        # 3. Insert dữ liệu mới
        print("   📥 Inserting new batch...")
        con.execute(f"""
            INSERT INTO fact_merged_features BY NAME
            SELECT * FROM read_parquet('{duck_read_path}', hive_partitioning=1)
        """)
        print("   ✅ Sync Complete.")

    except Exception as e:
        if "No files found" in str(e):
            print("   ⚠️ No new files to sync.")
        else:
            print(f"   ❌ DuckDB Error: {e}")
    finally:
        con.close()
def process_and_merge_all(spark):

    print("\n🚀 Starting Unified Merger Process...")
    all_interval_dfs = []

    for interval in INTERVALS:
        # Hàm này giờ đã trả về DataFrame (hoặc None)
        df = merge_features_for_interval(spark, interval)
        if df is not None:
            all_interval_dfs.append(df)

    if not all_interval_dfs:
        print("❌ No data found for any interval.")
        return False

    print(f"\n🔗 Unioning {len(all_interval_dfs)} intervals into one DataFrame...")

    # Union tất cả lại
    final_big_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), all_interval_dfs)

    print(f"💾 Writing unified dataset to: {PATH_GOLD_STAGING}")

    # Ghi 1 lần duy nhất, Partition theo cả date_part và interval
    final_big_df.write \
        .mode("overwrite") \
        .partitionBy("date_part") \
        .parquet(PATH_GOLD_STAGING)

    # Gọi hàm Sync mới
    sync_unified_to_duckdb(PATH_GOLD_STAGING)
    return True
def merge_features(spark):
    spark.sparkContext.setLogLevel("ERROR")
    print("===========================================")
    print("   GOLD LAYER: FEATURE MERGER PIPELINE    ")
    print("===========================================")
    process_and_merge_all(spark)
    print("\n🏁 Pipeline Finished.")
#merge_features()