import duckdb
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from plugins.spark_config import S3_BUCKET, DUCKDB_PATH

#STAGING_OUTPUT_PATH = f"s3a://{S3_BUCKET}/silver/agg_orderbook/"

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

def get_last_processed_time(interval_name):
    con = duckdb.connect(DUCKDB_PATH)
    try:
        table_exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'fact_orderbook_features'").fetchone()[0]
        if table_exists == 0: return None

        query = f"SELECT MAX(candle_time) FROM fact_orderbook_features WHERE interval = '{interval_name}'"
        result = con.execute(query).fetchone()[0]
        return result
    except:
        return None
    finally:
        con.close()

def calculate_orderbook_features(spark, interval_name, interval_window):
    print(f"\n📚 Processing Orderbook: {interval_name} ({interval_window})")
    interval_minutes = parse_interval_to_minutes(interval_name)
    buffer_minutes = (interval_minutes * 2) + 10
    last_time = get_last_processed_time(interval_name)
    # Cutoff Time: Thời gian hiện tại trừ 1 phút (để chắc chắn data đã về)
    # Chỉ xử lý các window kết thúc TRƯỚC thời điểm này
    cutoff_time = datetime.now() - timedelta(minutes=1)

    input_path = f"s3a://{S3_BUCKET}/silver/order_books/"
    df = spark.read.parquet(input_path)

    if last_time:
        start_timestamp = last_time - timedelta(minutes=buffer_minutes)
        start_date_str = start_timestamp.strftime("%Y-%m-%d")

        print(f"   ℹ️ Mode: Incremental (Reading >= {start_timestamp})")

        # Partition Pruning (Lọc Folder)
        df = df.filter(F.col("date_part") >= F.lit(start_date_str))
        # Row Filtering (Lọc Time)
        df = df.filter(F.col("snapshot_time") >= F.lit(start_timestamp))
    else:
        print("   ℹ️ Mode: Full Load")
    try:
        # Lấy dòng có thời gian nhỏ nhất trong batch hiện tại
        min_row = df.select(F.min("snapshot_time")).collect()
        min_data_time = min_row[0][0]

        if min_data_time is None:
            print("   ⚠️ No data found in this range.")
            return False

        print(f"   🕒 Batch Min Data Time: {min_data_time}")
    except Exception as e:
        print(f"   ⚠️ Error checking min time: {e}")
        return False
    # ---------------------------------------------------------
    # BƯỚC 1: SNAPSHOT AGGREGATION (Tái tạo sổ lệnh tại mỗi giây)
    # ---------------------------------------------------------
    # Vì dữ liệu Silver đã bị explode (mỗi dòng 1 mức giá), ta phải gom lại
    # để tính toán Best Bid, Best Ask, Total Depth... cho từng snapshot_time

    snapshot_df = df.groupBy("symbol", "snapshot_time").agg(
        # Best Price
        F.max(F.when(F.col("side") == "bid", F.col("price"))).alias("best_bid"),
        F.min(F.when(F.col("side") == "ask", F.col("price"))).alias("best_ask"),

        # Total Volume (Depth)
        F.sum(F.when(F.col("side") == "bid", F.col("quantity"))).alias("sum_bid"),
        F.sum(F.when(F.col("side") == "ask", F.col("quantity"))).alias("sum_ask"),

        # Weighted Price components (Total Money = Price * Qty)
        F.sum(F.col("price") * F.col("quantity")).alias("total_turnover"),
        F.sum("quantity").alias("total_qty")
    )

    # Tính các Derived Features (Giống Polars)
    # 1. Spread
    snapshot_df = snapshot_df.withColumn("spread", F.col("best_ask") - F.col("best_bid"))

    # 2. Mid Price
    snapshot_df = snapshot_df.withColumn("mid_price", (F.col("best_ask") + F.col("best_bid")) / 2)

    # 3. Weighted Mid Price (WMP) ~ total_turnover / total_qty
    snapshot_df = snapshot_df.withColumn("wmp", F.col("total_turnover") / F.col("total_qty"))

    # 4. Imbalance (Bid / (Bid + Ask))
    snapshot_df = snapshot_df.withColumn("imbalance",
        F.col("sum_bid") / (F.col("sum_bid") + F.col("sum_ask") + 1e-9)
    )

    # 5. Book Pressure (WMP - MidPrice)
    snapshot_df = snapshot_df.withColumn("book_pressure", F.col("wmp") - F.col("mid_price"))

    # ---------------------------------------------------------
    # BƯỚC 2: WINDOW AGGREGATION (Gom snapshot thành nến OHLC)
    # ---------------------------------------------------------
    features_df = snapshot_df.groupBy(
        F.col("symbol"),
        F.window(F.col("snapshot_time"), interval_window).alias("window")
    ).agg(
        # --- A. MID PRICE OHLC ---
        F.min(F.struct("snapshot_time", "mid_price")).getItem("mid_price").alias("mid_open"),
        F.max("mid_price").alias("mid_high"),
        F.min("mid_price").alias("mid_low"),
        F.max(F.struct("snapshot_time", "mid_price")).getItem("mid_price").alias("mid_close"),

        # --- B. WMP STATS (Weighted Mid Price) ---
        F.avg("wmp").alias("wmp_mean"),
        F.stddev("wmp").alias("wmp_std"),

        # --- C. SPREAD STATS ---
        F.avg("spread").alias("spread_mean"),
        F.max("spread").alias("spread_max"),

        # --- D. IMBALANCE STATS ---
        F.avg("imbalance").alias("imbal_mean"),
        F.min("imbalance").alias("imbal_min"),
        F.max("imbalance").alias("imbal_max"),
        F.percentile_approx("imbalance", 0.5).alias("imbal_median"),

        # --- E. BOOK PRESSURE ---
        F.avg("book_pressure").alias("pressure_mean"),
        F.stddev("book_pressure").alias("pressure_std"),

        # --- F. DEPTH STATS ---
        F.avg("sum_bid").alias("depth_bid_mean"),
        F.avg("sum_ask").alias("depth_ask_mean"),

        # Count
        F.count("*").alias("snapshot_count")
    )

    # ---------------------------------------------------------
    # BƯỚC 3: STRICT FILTER (Chỉ lấy nến đã đóng)
    # ---------------------------------------------------------
    features_df = features_df.filter(
        (F.col("window.end") <= F.lit(cutoff_time)) &
        (F.col("window.start") >= F.lit(min_data_time))
    )
    if features_df.rdd.isEmpty():
        print(f"   ⚠️ No closed candles found for {interval_name}. Waiting for data...")
        return False

    final_df = features_df.select(
        F.col("symbol"),
        F.col("window.start").alias("candle_time"),
        F.lit(interval_name).alias("interval"),

        "mid_open", "mid_high", "mid_low", "mid_close",
        "wmp_mean", "wmp_std",
        "spread_mean", "spread_max",
        "imbal_mean", "imbal_min", "imbal_max", "imbal_median",
        "pressure_mean", "pressure_std",
        "depth_bid_mean", "depth_ask_mean",
        "snapshot_count",
        F.current_timestamp().alias("ingestion_time")
    )
    final_df = final_df.withColumn("date_part", F.date_format("candle_time", "yyyy-MM-dd"))
    # 4. WRITE TO STAGING (Overwriting interval specific folder)
    staging_path = f"s3a://{S3_BUCKET}/silver/agg_orderbook/{interval_name}"
    print(f"   💾 Writing to Staging: {staging_path}")
    final_df.write.mode("overwrite").parquet(staging_path)
    return True

def merge_to_duckdb(interval_name):
    print(f"🦆 Merging Orderbook {interval_name} into DuckDB...")
    con = duckdb.connect(DUCKDB_PATH)

    # Config MinIO
    with open('/opt/airflow/SQL_db/config_dw/warehouse_source.sql', 'r') as f:
        sql_script = f.read()
    con.execute(sql_script)

    staging_source = f"s3://{S3_BUCKET}/silver/agg_orderbook/{interval_name}/*.parquet"

    try:
        # Create Table (Schema to lớn)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS fact_orderbook_features AS
            SELECT * FROM read_parquet('{staging_source}') LIMIT 0
        """)

        # Clean overlapping data
        print("   🔄 Cleaning overlapping data...")
        con.execute(f"""
            DELETE FROM fact_orderbook_features
            WHERE interval = '{interval_name}'
            AND (symbol, candle_time) IN (
                SELECT symbol, candle_time FROM read_parquet('{staging_source}')
            )
        """)

        # Insert new data
        print("   📥 Inserting new data...")
        con.execute(f"""
            INSERT INTO fact_orderbook_features
            SELECT * FROM read_parquet('{staging_source}')
        """)
        print("   ✅ Merge Complete.")

    except Exception as e:
        if "No files found" in str(e):
            print(f"   ℹ️ No new data for {interval_name}.")
        else:
            print(f"   ⚠️ DuckDB Error: {e}")
    finally:
        con.close()

def agg_orderbook(spark):
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "parallel_pool")
    intervals = [
        ("1m", "1 minute"),
        ("5m", "5 minutes"),
        ("15m", "15 minutes"),
        ("1h", "1 hour"),
        ("4h", "4 hours")
    ]

    print(f"STARTING ORDERBOOK BATCH PIPELINE...")

    for name, window in intervals:
        has_data = calculate_orderbook_features(spark, name, window)
        if has_data:
            merge_to_duckdb(name)
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)
#agg_orderbook(spark)