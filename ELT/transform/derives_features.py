import sys
import duckdb
from datetime import timedelta
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from plugins.spark_config import S3_BUCKET, DUCKDB_PATH

STAGING_OUTPUT_PATH = f"s3a://{S3_BUCKET}/gold/staging_derive"
FINAL_OUTPUT_PATH = f"s3a://{S3_BUCKET}/gold/derive_final"
INPUT_PATH = f"s3a://{S3_BUCKET}/gold/staging_merged_features/"
INTERVALS = ["5m", "15m", "1h", "4h", "1d"]

def get_last_processed_time():
    con = duckdb.connect(DUCKDB_PATH)
    try:
        with open('/opt/airflow/SQL_db/config_dw/warehouse_source.sql', 'r') as f:
            sql_script = f.read()
        con.execute(sql_script)

        table_exists = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'fact_derived_features'"
        ).fetchone()[0]

        if table_exists == 0:
            return None

        result = con.execute(
            "SELECT MAX(timestamp) FROM fact_derived_features"
        ).fetchone()[0]
        return result
    except Exception as e:
        print(f"Warning: Could not get last processed time: {e}")
        return None
    finally:
        con.close()
def load_data(spark):
    """Load merged data từ MinIO với incremental nếu có"""
    last_time = get_last_processed_time()
    df = spark.read.parquet(INPUT_PATH)

    if last_time:
        start_timestamp = last_time - timedelta(days=1)  # Buffer 1 ngày
        print(f" ℹ️ Incremental load: timestamp >= {start_timestamp}")
        df = df.filter(F.col("timestamp") >= F.lit(start_timestamp))

    if df.rdd.isEmpty():
        print("Error: No data loaded")
        return None

    # Chuẩn hóa timestamp
    df = df.withColumn("timestamp", F.col("timestamp").cast(TimestampType()))

    row_count = df.count()
    print(f"✓ Loaded input data: {row_count} rows")
    return df
def create_macro_basis_features(df):
    """Tạo Macro & Basis features - giống Polars"""
    print("🔧 Creating Macro & Basis features...")
    for interval in INTERVALS:
        prefix_trade = f"trades_{interval}_"
        prefix_index = f"index_price_{interval}_"
        prefix_mark = f"mark_price_{interval}_"

        price_mean_trade = f"{prefix_trade}price_mean_trade"
        index_mean = f"{prefix_index}mean"
        mark_mean = f"{prefix_mark}mean"
        price_std_trade = f"{prefix_trade}price_std_trade"
        index_std = f"{prefix_index}std"

        if all(c in df.columns for c in [price_mean_trade, index_mean, mark_mean]):
            df = df.withColumn(f"feat_basis_spread_{interval}", F.col(price_mean_trade) - F.col(index_mean))
            df = df.withColumn(f"feat_basis_ratio_{interval}", (F.col(price_mean_trade) - F.col(index_mean)) / (F.col(index_mean) + 1e-9))
            df = df.withColumn(f"feat_premium_index_{interval}", (F.col(mark_mean) - F.col(index_mean)) / (F.col(index_mean) + 1e-9))
            if price_std_trade in df.columns and index_std in df.columns:
                df = df.withColumn(f"feat_volatility_spread_{interval}", F.col(price_std_trade) - F.col(index_std))
    return df
def create_log_returns(df):
    """Tạo log returns và trend divergence - giống Polars"""
    print("🔧 Creating log returns...")
    window = Window.orderBy("timestamp")

    for interval in INTERVALS:
        prefix_orderbook = f"orderbook_{interval}_"
        prefix_index = f"index_price_{interval}_"

        wmp_last = f"{prefix_orderbook}wmp_last"
        index_close = f"{prefix_index}close"

        if wmp_last in df.columns:
            df = df.withColumn(f"feat_log_return_trade_{interval}", F.log(F.col(wmp_last) / (F.lag(F.col(wmp_last), 1).over(window) + 1e-9)))

        if index_close in df.columns:
            df = df.withColumn(f"feat_log_return_index_{interval}", F.log(F.col(index_close) / (F.lag(F.col(index_close), 1).over(window) + 1e-9)))

        # Trend divergence sau khi có log returns
        trade_ret = f"feat_log_return_trade_{interval}"
        index_ret = f"feat_log_return_index_{interval}"
        if trade_ret in df.columns and index_ret in df.columns:
            df = df.withColumn(f"feat_trend_divergence_{interval}", F.col(trade_ret) - F.col(index_ret))

    return df
def create_funding_sentiment_features(df):
    """Tạo funding sentiment features - giống Polars"""
    print("🔧 Creating Funding & Sentiment features...")
    window_12 = Window.orderBy("timestamp").rowsBetween(-11, 0)  # 12 kỳ

    for interval in INTERVALS:
        prefix_orderbook = f"orderbook_{interval}_"

        wmp_last = f"{prefix_orderbook}wmp_last"
        funding_rate = "funding_funding_rate"
        basis_ratio = f"feat_basis_ratio_{interval}"

        if wmp_last in df.columns and funding_rate in df.columns:
            df = df.withColumn(f"feat_funding_cost_{interval}", F.col(funding_rate) * F.col(wmp_last))
            df = df.withColumn(f"feat_funding_trend_{interval}", F.col(funding_rate) - F.mean(funding_rate).over(window_12))

        if basis_ratio in df.columns:
            df = df.withColumn(f"feat_funding_basis_corr_{interval}", F.col(funding_rate) * F.col(basis_ratio))

    return df
def create_momentum_trend_features(df):
    """Tạo momentum features - giống Polars"""
    print("🔧 Creating Momentum & Trend features...")
    window_3 = Window.orderBy("timestamp").rowsBetween(-2, 0)  # 3 kỳ
    window_12 = Window.orderBy("timestamp").rowsBetween(-11, 0)  # 12 kỳ

    for interval in INTERVALS:
        prefix_orderbook = f"orderbook_{interval}_"

        wmp_last = f"{prefix_orderbook}wmp_last"
        wmp_min = f"{prefix_orderbook}wmp_min"
        wmp_max = f"{prefix_orderbook}wmp_max"

        if all(c in df.columns for c in [wmp_last, wmp_min, wmp_max]):
            df = df.withColumn(f"feat_price_velocity_{interval}", (F.col(wmp_last) - F.lag(F.col(wmp_last), 3)) / 3)
            df = df.withColumn(f"feat_ma_divergence_{interval}", F.col(wmp_last) / (F.mean(wmp_last).over(window_12) + 1e-9) - 1)
            df = df.withColumn(f"feat_rsi_proxy_{interval}", (F.col(wmp_last) - F.col(wmp_min)) / (F.col(wmp_max) - F.col(wmp_min) + 1e-9))

            # Acceleration sau velocity
            velocity_col = f"feat_price_velocity_{interval}"
            df = df.withColumn(f"feat_price_accel_{interval}", F.col(velocity_col) - F.lag(F.col(velocity_col), 1))

    return df
def create_order_flow_features(df):
    """Tạo order flow features - giống Polars"""
    print("🔧 Creating Order Flow features...")
    for interval in INTERVALS:
        prefix_trades = f"trades_{interval}_"
        prefix_orderbook = f"orderbook_{interval}_"

        volume_buy = f"{prefix_trades}volume_buy"
        volume_sell = f"{prefix_trades}volume_sell"
        volume_total = f"{prefix_trades}volume_total"
        count_buy = f"{prefix_trades}count_buy"
        count_sell = f"{prefix_trades}count_sell"

        sum_bid_50 = f"{prefix_orderbook}sum_bid_50"
        sum_ask_50 = f"{prefix_orderbook}sum_ask_50"
        total_depth_50 = f"{prefix_orderbook}total_depth_50"

        if all(c in df.columns for c in [volume_buy, volume_sell, volume_total]):
            df = df.withColumn(f"feat_trade_imbalance_{interval}", (F.col(volume_buy) - F.col(volume_sell)) / (F.col(volume_total) + 1e-9))

        if all(c in df.columns for c in [sum_bid_50, sum_ask_50, total_depth_50]):
            df = df.withColumn(f"feat_depth_imbalance_{interval}", (F.col(sum_bid_50) - F.col(sum_ask_50)) / (F.col(total_depth_50) + 1e-9))
            df = df.withColumn(f"feat_buy_consumption_{interval}", F.col(volume_buy) / (F.col(sum_ask_50) + 1e-9))
            df = df.withColumn(f"feat_sell_consumption_{interval}", F.col(volume_sell) / (F.col(sum_bid_50) + 1e-9))

        if all(c in df.columns for c in [count_buy, count_sell]):
            df = df.withColumn(f"feat_aggressiveness_{interval}", F.col(count_buy) / (F.col(count_sell) + 1e-9))

        # Smart money divergence sau imbalance
        trade_imbal = f"feat_trade_imbalance_{interval}"
        depth_imbal = f"feat_depth_imbalance_{interval}"
        if trade_imbal in df.columns and depth_imbal in df.columns:
            df = df.withColumn(f"feat_smart_money_div_{interval}", F.col(trade_imbal) - F.col(depth_imbal))

    return df
def create_volatility_liquidity_features(df):
    """Tạo volatility & liquidity - giống Polars"""
    print("🔧 Creating Volatility & Liquidity features...")
    for interval in INTERVALS:
        prefix_orderbook = f"orderbook_{interval}_"
        prefix_trades = f"trades_{interval}_"

        spread_mean = f"{prefix_orderbook}spread_mean"
        wmp_mean = f"{prefix_orderbook}wmp_mean"
        total_depth_50 = f"{prefix_orderbook}total_depth_50"

        price_max_trade = f"{prefix_trades}price_max_trade"
        price_min_trade = f"{prefix_trades}price_min_trade"
        price_mean_trade = f"{prefix_trades}price_mean_trade"
        price_last_trade = f"{prefix_trades}price_last_trade"

        if spread_mean in df.columns and wmp_mean in df.columns:
            df = df.withColumn(f"feat_rel_spread_{interval}", F.col(spread_mean) / (F.col(wmp_mean) + 1e-9))

        if total_depth_50 in df.columns and spread_mean in df.columns:
            df = df.withColumn(f"feat_liq_density_{interval}", F.col(total_depth_50) / (F.col(spread_mean) + 1e-9))

        if all(c in df.columns for c in [price_max_trade, price_min_trade, price_mean_trade, price_last_trade]):
            df = df.withColumn(f"feat_candle_range_{interval}", (F.col(price_max_trade) - F.col(price_min_trade)) / (F.col(price_mean_trade) + 1e-9))
            df = df.withColumn(f"feat_tail_extension_{interval}", (F.col(price_max_trade) - F.col(price_last_trade)) / (F.col(price_max_trade) - F.col(price_min_trade) + 1e-9))

    return df
def create_time_features(df):
    """Tạo time features - giống Polars"""
    print("🔧 Creating Time features...")
    df = df.withColumn("hour", F.hour("timestamp"))
    df = df.withColumn("feat_hour_sin", F.sin(2 * 3.141592653589793 * F.col("hour") / 24))
    df = df.withColumn("feat_hour_cos", F.cos(2 * 3.141592653589793 * F.col("hour") / 24))
    return df.drop("hour")
def create_efficiency_features(df):
    """Tạo efficiency features - giống Polars"""
    print("🔧 Creating Market Efficiency features...")
    window_12 = Window.orderBy("timestamp").rowsBetween(-11, 0)

    for interval in INTERVALS:
        prefix_orderbook = f"orderbook_{interval}_"
        wmp_last = f"{prefix_orderbook}wmp_last"

        if wmp_last in df.columns:
            df = df.withColumn("price_diff", F.abs(F.col(wmp_last) - F.lag(F.col(wmp_last), 1)))
            df = df.withColumn(f"feat_efficiency_ratio_{interval}", F.abs(F.col(wmp_last) - F.lag(F.col(wmp_last), 12)) / (F.sum("price_diff").over(window_12) + 1e-9))
            df = df.withColumn(f"feat_price_entropy_{interval}", F.stddev("wmp_last").over(window_12) / (F.mean("wmp_last").over(window_12) + 1e-9))
    return df.drop("price_diff") if "price_diff" in df.columns else df
def create_orderbook_shape_features(df):
    """Tạo orderbook shape - giống Polars"""
    print("🔧 Creating Orderbook Shape features...")
    for interval in INTERVALS:
        prefix = f"orderbook_{interval}_"

        sum_bid_20 = f"{prefix}sum_bid_20"
        sum_ask_20 = f"{prefix}sum_ask_20"
        sum_bid_5 = f"{prefix}sum_bid_5"
        sum_bid_50 = f"{prefix}sum_bid_50"
        best_bid = f"{prefix}best_bid"
        best_ask = f"{prefix}best_ask"
        bid_px_20 = f"{prefix}bid_px_20"
        ask_px_20 = f"{prefix}ask_px_20"

        if all(c in df.columns for c in [sum_bid_20, sum_ask_20, best_bid, best_ask, bid_px_20, ask_px_20]):
            df = df.withColumn(f"feat_bid_slope_{interval}", F.col(sum_bid_20) / (F.col(best_bid) - F.col(bid_px_20) + 1e-9))
            df = df.withColumn(f"feat_ask_slope_{interval}", F.col(sum_ask_20) / (F.col(ask_px_20) - F.col(best_ask) + 1e-9))
            bid_slope = f"feat_bid_slope_{interval}"
            ask_slope = f"feat_ask_slope_{interval}"
            df = df.withColumn(f"feat_slope_imbalance_{interval}", F.col(bid_slope) / (F.col(ask_slope) + 1e-9))

        if all(c in df.columns for c in [sum_bid_5, sum_bid_50]):
            df = df.withColumn(f"feat_depth_convexity_{interval}", F.col(sum_bid_5) / (F.col(sum_bid_50) + 1e-9))

    return df
def create_statistical_normalization(df):
    """Tạo Z-scores - giống Polars"""
    print("🔧 Creating Statistical Normalization features...")
    window_1h = Window.orderBy("timestamp").rowsBetween(-11, 0)  # 12 kỳ = 1h cho 5m base

    for interval in INTERVALS:
        prefix_trades = f"trades_{interval}_"
        prefix_orderbook = f"orderbook_{interval}_"

        volume_total = f"{prefix_trades}volume_total"
        spread_mean = f"{prefix_orderbook}spread_mean"
        trade_imbalance = f"feat_trade_imbalance_{interval}"

        if volume_total in df.columns:
            df = df.withColumn(f"feat_z_volume_{interval}", (F.col(volume_total) - F.mean(volume_total).over(window_1h)) / (F.stddev(volume_total).over(window_1h) + 1e-9))

        if spread_mean in df.columns:
            df = df.withColumn(f"feat_z_spread_{interval}", (F.col(spread_mean) - F.mean(spread_mean).over(window_1h)) / (F.stddev(spread_mean).over(window_1h) + 1e-9))

        if trade_imbalance in df.columns:
            df = df.withColumn(f"feat_z_imbalance_{interval}", (F.col(trade_imbalance) - F.mean(trade_imbalance).over(window_1h)) / (F.stddev(trade_imbalance).over(window_1h) + 1e-9))

    return df
def create_vwap_pivot_features(df):
    """Tạo VWAP & Pivot features - giống Polars"""
    print("🔧 Creating VWAP & Pivot features...")
    window_1h = Window.orderBy("timestamp").rowsBetween(-11, 0)

    for interval in INTERVALS:
        prefix_orderbook = f"orderbook_{interval}_"
        prefix_trades = f"trades_{interval}_"

        wmp_last = f"{prefix_orderbook}wmp_last"
        vwap = f"{prefix_trades}vwap"  # Giả định có vwap từ trades

        if wmp_last in df.columns and vwap in df.columns:
            df = df.withColumn(f"feat_dist_vwap_{interval}", (F.col(wmp_last) - F.col(vwap)) / (F.col(vwap) + 1e-9))
            df = df.withColumn(f"feat_dist_max_{interval}", (F.col(wmp_last) - F.max(wmp_last).over(window_1h)) / (F.max(wmp_last).over(window_1h) + 1e-9))
            df = df.withColumn(f"feat_dist_min_{interval}", (F.col(wmp_last) - F.min(wmp_last).over(window_1h)) / (F.min(wmp_last).over(window_1h) + 1e-9))

    return df
def create_all_features(df):
    """Gọi tuần tự tất cả feature creator - giống Polars"""
    print("🚀 Creating derived features...")
    df = df.orderBy("timestamp")

    df = create_log_returns(df)
    df = create_macro_basis_features(df)
    df = create_funding_sentiment_features(df)
    df = create_momentum_trend_features(df)
    df = create_order_flow_features(df)
    df = create_volatility_liquidity_features(df)
    df = create_time_features(df)
    df = create_efficiency_features(df)
    df = create_orderbook_shape_features(df)
    df = create_statistical_normalization(df)
    df = create_vwap_pivot_features(df)

    return df
def save_features_data(df):
    """Lưu staging + upsert DuckDB + lưu final gold - giống merge code"""
    if df is None:
        return

    # Thêm date_part để partition
    if "date_part" not in df.columns:
        df = df.withColumn("date_part", F.date_format("timestamp", "yyyy-MM-dd"))
    # Lưu staging
    print(f" 💾 Writing staging to {STAGING_OUTPUT_PATH}")
    df.write.mode("overwrite").partitionBy("date_part").parquet(STAGING_OUTPUT_PATH)

    # Upsert vào DuckDB
    con = duckdb.connect(DUCKDB_PATH)
    try:
        with open('/opt/airflow/SQL_db/config_dw/warehouse_source.sql', 'r') as f:
            sql_script = f.read()
        con.execute(sql_script)
        # Tạo bảng fact nếu chưa có
        con.execute(f"""
                    CREATE TABLE IF NOT EXISTS fact_derived_features AS
                    SELECT * EXCLUDE date_part 
                    FROM read_parquet('{STAGING_OUTPUT_PATH}/*/*.parquet', hive_partitioning=1) LIMIT 0
                """)

        # Xóa dữ liệu cũ (Chỉ trong khoảng thời gian batch hiện tại) -> Cực nhanh
        print(" 🔄 Cleaning overlapping data...")
        con.execute(f"""
                    DELETE FROM fact_derived_features
                    WHERE (timestamp) IN (
                        SELECT timestamp FROM read_parquet('{STAGING_OUTPUT_PATH}/*/*.parquet', hive_partitioning=1)
                    )
                """)

        # Insert dữ liệu mới (Chỉ lấy data trong khoảng batch hiện tại từ Staging)
        print(" 📥 Inserting new data...")
        con.execute(f"""
                    INSERT INTO fact_derived_features
                    SELECT * EXCLUDE date_part
                    FROM read_parquet('{STAGING_OUTPUT_PATH}/*/*.parquet', hive_partitioning=1)
                """)
        print(" ✅ DuckDB upsert completed.")
    except Exception as e:
        print(f" ⚠️ DuckDB Error: {e}")
    finally:
        con.close()

    # Lưu final gold (không partition)
    print(f" 💾 Final gold saved to {FINAL_OUTPUT_PATH}")
    df.drop("date_part").write.mode("append").parquet(FINAL_OUTPUT_PATH)
def derives_features(spark):
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "parallel_pool")
    print("STARTING DERIVED FEATURES PIPELINE...")

    try:
        df = load_data(spark)
        if df is None:
            print("No data loaded. Exiting.")
            sys.exit(1)

        enhanced_df = create_all_features(df)
        save_features_data(enhanced_df)
        print("PIPELINE COMPLETED SUCCESSFULLY!")
    except Exception as e:
        print(f"Pipeline failed: {e}")
        sys.exit(1)
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)
#derives_features()