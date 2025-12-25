import duckdb
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import  TimestampType
from plugins.spark_config import S3_BUCKET, DUCKDB_PATH
STAGING_OUTPUT_PATH = f"s3a://{S3_BUCKET}/silver/indexPriceKlines/"

def parse_interval_to_minutes(interval_str):
    unit = interval_str[-1].lower()
    try:
        value = int(interval_str[:-1])
    except:
        return 60
    if unit == 'm': return value
    if unit == 'h': return value * 60
    if unit == 'd': return value * 1440
    return 60

def parse_interval_to_ms(interval_str):
    """Đổi sang ms"""
    return parse_interval_to_minutes(interval_str) * 60 * 1000

def get_last_processed_time(interval_name):
    con = duckdb.connect(DUCKDB_PATH)
    try:
        table_exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'fact_indexpriceklines_features'").fetchone()[0]
        if table_exists == 0: return None
        query = f"SELECT MAX(close_time) FROM fact_indexpriceklines_features WHERE interval = '{interval_name}'"
        result = con.execute(query).fetchone()[0]
        return result
    except:
        return None
    finally:
        con.close()
def calculate_indexpriceklines_features(spark, interval_name, interval_window_ms):
    print(f"\n📈 Processing IndexPriceKlines: {interval_name} (window: {interval_window_ms / 1000 / 60} minutes)")
    # 1. SETUP THỜI GIAN (Incremental + Buffer)
    interval_ms = parse_interval_to_ms(interval_name)
    buffer_ms = interval_ms * 2  # Buffer gấp đôi interval để an toàn
    last_time = get_last_processed_time(interval_name)
    cutoff_time = datetime.now() - timedelta(minutes=1)  # Không lấy nến hiện tại
    input_path = f"s3a://{S3_BUCKET}/silver/ohlc_index/"
    # Đọc dữ liệu từ MinIO bronze (đã nén jsonl.gz, partitioned date/hour)
    df = spark.read.parquet(input_path)

    df = df.filter(F.col("channel") == f"index-candle{interval_name}")

    # Transform raw thành 1m klines (giống ETL)
    df = df.withColumnRenamed("candle_time", "close_time")
    # Giả định time_col là close_time
    time_col = col("close_time")
    if last_time:
        start_timestamp = last_time - timedelta(milliseconds=buffer_ms)
        start_date_str = start_timestamp.strftime("%Y-%m-%d")
        prev_date_str = (start_timestamp - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f" ℹ️ Incremental Mode: Reading >= {start_timestamp}, including prev day {prev_date_str}")
        # Filter theo thời gian (Spark sẽ pruning nếu partitioned đúng)
        df = df.filter(time_col >= F.lit(start_timestamp))
    else:
        print(" ℹ️ Mode: Full Load")
    # 2. CHECK MIN DATA TIME (Để chặn nến thiếu đầu - giống Polars strict filter)
    try:
        min_row = df.select(F.min(time_col).alias("min_time")).collect()[0]
        min_data_time = min_row["min_time"]
        if min_data_time is None:
            print(" ⚠️ No data found in this range.")
            return False
        print(f" 🕒 Batch Min Data Time: {min_data_time}")
    except Exception as e:
        print(f" ⚠️ Error checking min time: {e}")
        return False
    # 3. WINDOW AGGREGATION (Resample giống Polars)
    interval_minutes = int(interval_ms / 1000 / 60)
    interval_window = f"{interval_minutes} minutes"
    features_df = df.groupBy(
        F.col("symbol"),
        F.window(time_col.cast(TimestampType()), interval_window).alias("window")
    ).agg(
        # OHLC - giống hệt Polars
        F.first("open").alias("open"),  # First open in window
        F.max("high").alias("high"),
        F.min("low").alias("low"),
        F.last("close").alias("close"),  # Last close in window

        # Stats - giống Polars
        F.avg("close").alias("mean"),
        F.stddev("close").alias("std")
    )

    # 4. STRICT FILTER (Closed candles only - giống Polars)
#    cutoff_ms = cutoff_time.timestamp() * 1000
 #   min_data_ms = min_data_time if isinstance(min_data_time, int) else min_data_time.timestamp() * 1000
    cutoff_timestamp = cutoff_time
    features_df = features_df.filter(
        (F.col("window.end") <= F.lit(cutoff_timestamp)) &
        (F.col("window.start") >= F.lit(min_data_time))
    )

    if features_df.rdd.isEmpty():
        print(f" ⚠️ No closed candles found for {interval_name}.")
        return False

    # Thêm metadata và partition column
    final_df = features_df.select(
        F.col("symbol"),
        F.col("window.end").alias("close_time"),  # Sử dụng end làm timestamp_dt
        F.lit(interval_name).alias("interval"),
        #F.col('channel').alias("interval"),
        "open", "high", "low", "close", "mean", "std",
        F.current_timestamp().alias("ingestion_time")
    ).withColumn(
        "date_part",
        F.date_format(F.col("close_time"), "yyyy-MM-dd")
    )

    # 5. WRITE TO STAGING (MinIO) - partitioned by date_part giống silver ETL
    staging_path = f"{STAGING_OUTPUT_PATH}/{interval_name}"
    print(f" 💾 Writing to Staging: {staging_path}")
    final_df.write.mode("append").partitionBy("date_part").parquet(staging_path)
    return True

def merge_to_duckdb(interval_name):
    print(f"🦆 Merging IndexPriceKlines {interval_name} into DuckDB...")
    con = duckdb.connect(DUCKDB_PATH)

    # Config MinIO cho DuckDB
    with open('/opt/airflow/SQL_db/config_dw/warehouse_source.sql', 'r') as f:
        sql_script = f.read()
    con.execute(sql_script)

    staging_source = f"s3://{S3_BUCKET}/silver/indexPriceKlines/{interval_name}/*/*.parquet"  # Đọc partitioned parquet

    try:
        # Create Fact Table nếu chưa có
        con.execute("""
            CREATE TABLE IF NOT EXISTS fact_indexpriceklines_features (
                symbol VARCHAR,
                close_time TIMESTAMP,
                interval VARCHAR,
                open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
                mean DOUBLE, std DOUBLE,
                ingestion_time TIMESTAMP
            )
        """)

        # Upsert Logic (Delete old + Insert new) - giống incremental Polars
        print(" 🔄 Cleaning overlapping data...")
        con.execute(f"""
            DELETE FROM fact_indexpriceklines_features
            WHERE interval = '{interval_name}'
            AND (symbol, close_time) IN (
                SELECT symbol, close_time FROM read_parquet('{staging_source}', hive_partitioning=1)
            )
        """)

        print(" 📥 Inserting new data...")
        con.execute(f"""
            INSERT INTO fact_indexpriceklines_features
            SELECT
                symbol,
                close_time,
                interval,
                open, high, low, close,
                mean, std,
                ingestion_time
            FROM read_parquet('{staging_source}', hive_partitioning=1)
        """)
        print(" ✅ Merge Complete.")
    except Exception as e:
        if "No files found" in str(e):
            print(f" ℹ️ No new data for {interval_name}.")
        else:
            print(f" ⚠️ DuckDB Error: {e}")
    finally:
        con.close()

def index_price(spark):
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "parallel_pool")
    intervals = [
        ("1m", parse_interval_to_ms("1m")),
        ("5m", parse_interval_to_ms("5m")),
        ("15m", parse_interval_to_ms("15m")),
        ("1H", parse_interval_to_ms("1H")),
        ("4H", parse_interval_to_ms("4H")),
        ("1D", parse_interval_to_ms("1D"))
    ]

    print("STARTING INDEXPRICEKLINES BATCH PIPELINE...")
    for name, window_ms in intervals:
        has_data = calculate_indexpriceklines_features(spark, name, window_ms)
        if has_data:
            merge_to_duckdb(name)
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)
#index_price()