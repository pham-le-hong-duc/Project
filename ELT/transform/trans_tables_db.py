import duckdb
from pyspark.sql.functions import col, explode, to_timestamp, date_format, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from plugins.spark_config import S3_BUCKET, DUCKDB_PATH
def get_duckdb_conn():
    con = duckdb.connect(DUCKDB_PATH)
    with open('/opt/airflow/SQL_db/config_dw/warehouse_source.sql', 'r') as f:
         sql_script = f.read()
    con.execute(sql_script)
    return con
def get_schemas():
    trade_element = StructType([
        StructField("instId", StringType(), True),
        StructField("tradeId", StringType(), True),
        StructField("px", StringType(), True),
        StructField("sz", StringType(), True),
        StructField("side", StringType(), True),
        StructField("ts", StringType(), True)
    ])
    funding_element = StructType([
        StructField("instId", StringType(), True),
        StructField("fundingRate", StringType(), True),
        StructField("nextFundingRate", StringType(), True),
        StructField("fundingTime", StringType(), True),
        StructField("nextFundingTime", StringType(), True)
    ])
    ohlc_element = ArrayType(StringType())
    book_element = StructType([

        StructField("asks", ArrayType(ArrayType(StringType())), True),
        StructField("bids", ArrayType(ArrayType(StringType())), True),
        #StructField("instId", StringType(), True),
        StructField("ts", StringType(), True)
    ])
    return {
        "trades": trade_element,
        "funding": funding_element,
        "ohlc": ohlc_element,
        "book": book_element
    }
def get_raw_schema(data_schema):
    return StructType([
        StructField("received_at", StringType(), True),
        StructField("payload", StructType([
            StructField("data", ArrayType(data_schema), True),
            StructField("arg", StructType([
                StructField("channel", StringType(), True),
                StructField("instId", StringType(), True)
            ]), True)
        ]), True)
    ])
def transform_trades(df):
    return df.select(
        col("received_at"), explode(col("payload.data")).alias("data")
    ).select(
        col("data.instId").alias("symbol"),
        col("data.tradeId").alias("tradeId"),
        col("data.side").alias("side"),
        col("data.px").cast("double").alias("price"),
        col("data.sz").cast("double").alias("quantity"),
        (col("data.ts").cast("long") / 1000).cast("timestamp").alias("trade_time"),
        col("received_at").cast("timestamp").alias("ingestion_time")
    ).dropDuplicates(["tradeId"])
def transform_funding(df_raw):
    return df_raw.select(
        col("received_at"), explode(col("payload.data")).alias("data")
    ).select(
        col("data.instId").alias("symbol"),
        lit("SWAP").alias("instrument_type"), # Funding thường là SWAP
        col("data.fundingRate").cast("double").alias("funding_rate"),
        col("data.nextFundingRate").cast("double").alias("next_funding_rate"),
        (col("data.fundingTime").cast("long") / 1000).cast("timestamp").alias("funding_time"),
        (col("data.nextFundingTime").cast("long") / 1000).cast("timestamp").alias("next_funding_time"),
        col("received_at").cast("timestamp").alias("ingestion_time")
    )
def transform_ohlc(df_raw, candle_type="mark"):
    # Index mapping: 0:ts, 1:o, 2:h, 3:l, 4:c
    return df_raw.select(
        col("received_at"),
        col("payload.arg.instId").alias("symbol"),
        col("payload.arg.channel").alias("channel"),
        explode(col("payload.data")).alias("c")
    ).select(
        col("symbol"),
        col("channel"),
        (col("c").getItem(0).cast("long") / 1000).cast("timestamp").alias("candle_time"),
        col("c").getItem(1).cast("double").alias("open"),
        col("c").getItem(2).cast("double").alias("high"),
        col("c").getItem(3).cast("double").alias("low"),
        col("c").getItem(4).cast("double").alias("close"),
        col("received_at").cast("timestamp").alias("ingestion_time")
        #lit(candle_type).alias("type")
    )
def transform_orderbook(df_raw):
    # Logic phức tạp cho orderbook (Explode Asks/Bids -> Union)
    df_exploded = df_raw.select(
        col("received_at"),
        col("payload.arg.instId").alias("symbol"),
        explode(col("payload.data")).alias("book")
    )
    # Process Asks
    df_asks = df_exploded.select(
        col("symbol"),
        col("book.ts").alias("ts"),
        col("received_at"),  # Giữ lại cột này
        explode(col("book.asks")).alias("asks"),
        lit("ask").alias("side")
    ).select(
        col("symbol"), col("side"),
        col("asks")[0].cast("double").alias("price"),
        col("asks")[1].cast("double").alias("quantity"),
        col("ts"), col("received_at")
    )

    # Process Bids
    df_bids = df_exploded.select(
        col("symbol"),
        col("book.ts").alias("ts"),
        col("received_at"),  # Giữ lại cột này
        explode(col("book.bids")).alias("bids"),
        lit("bid").alias("side")
    ).select(
        col("symbol"), col("side"),
        col("bids")[0].cast("double").alias("price"),
        col("bids")[1].cast("double").alias("quantity"),
        col("ts"), col("received_at")
    )

    # Union và chuẩn hóa time
    return df_asks.union(df_bids) \
        .withColumn("snapshot_time", (col("ts").cast("long") / 1000).cast("timestamp")) \
        .withColumn("ingestion_time", col("received_at").cast("timestamp")) \
        .drop("ts", "received_at")
    # Quan trọng: Dòng trên đổi tên received_at -> ingestion_time
def load_gold_layer(con, stg_table):
    print(f"   ✨ Loading Gold Layer for {stg_table}...")

    # 1. Update Dim Symbol
    con.execute(f"""
        INSERT INTO dim_symbol (symbol_code, base_currency, quote_currency)
        SELECT DISTINCT symbol, split_part(symbol, '-', 1), split_part(symbol, '-', 2)
        FROM {stg_table}
        WHERE symbol NOT IN (SELECT symbol_code FROM dim_symbol)
    """)
    # 3. Insert Fact Tables
    if stg_table == "trades":
        con.execute("""
                    INSERT INTO fact_trades (symbol_key, date_key, trade_id, price, quantity, trade_time)
                    SELECT d.symbol_key,
                           s.tradeId,
                           s.price,
                           s.quantity,
                           s.trade_time
                    FROM trades s
                             JOIN dim_symbol d ON s.symbol = d.symbol_code
                    WHERE s.trade_time > (SELECT COALESCE(MAX(trade_time), '1970-01-01'::TIMESTAMP) FROM fact_trades)
                    """)
    elif stg_table == "funding_rate":
        con.execute("""
                    INSERT INTO fact_funding_rate (symbol_key, date_key, funding_rate, next_funding_rate, funding_time)
                    SELECT d.symbol_key,
                           s.funding_rate,
                           s.next_funding_rate,
                           s.funding_time
                    FROM funding_rate s
                             JOIN dim_symbol d ON s.symbol = d.symbol_code
                    WHERE s.funding_time >
                          (SELECT COALESCE(MAX(funding_time), '1970-01-01'::TIMESTAMP) FROM fact_funding_rate)
                    """)

    print("   ✅ Gold Load Success")
def run_pipeline(spark, config_key, bronze_prefix, stg_table, transform_func, data_schema):
    print(f"\n🚀 Processing: {config_key} -> {stg_table}")

    # 1. Read Bronze
    try:
        df_raw = spark.read.schema(get_raw_schema(data_schema)).json(f"s3a://{S3_BUCKET}/{bronze_prefix}*/*/*.jsonl.gz")
        if df_raw.rdd.isEmpty(): return
    except:
        return

    # 2. Transform
    df_silver = transform_func(df_raw)

    # Partition Column Logic
    time_col = None
    for c in ["trade_time", "funding_time", "candle_time", "snapshot_time"]:
        if c in df_silver.columns: time_col = col(c); break

    df_silver = df_silver.withColumn(
        "date_part",
        date_format(time_col if time_col is not None else current_timestamp(), "yyyy-MM-dd")
    )

    # 3. Write Parquet
    silver_path = f"s3a://{S3_BUCKET}/silver/{stg_table}/"
    df_silver.write.mode("append").partitionBy("date_part").format("parquet").save(silver_path)
    print(f"💾 Saved Silver Parquet: {silver_path}")

    # 4. Load to DuckDB stg (Incremental)
    con = get_duckdb_conn()
    parquet_read_path = f"s3://{S3_BUCKET}/silver/{stg_table}/*/*.parquet"

    try:
        # Load Incremental dựa trên ingestion_time
        # Lưu ý: Bảng stg (ví dụ 'trades') cần được tạo trước trong DuckDB
        query = f"""
            INSERT INTO {stg_table} BY NAME
            SELECT * EXCLUDE (date_part)
            FROM read_parquet('{parquet_read_path}', hive_partitioning=1)
            WHERE ingestion_time > (SELECT COALESCE(MAX(ingestion_time), '1970-01-01'::TIMESTAMP) FROM {stg_table})
        """
        con.execute(query)
        print("✅ stg Load Success")

        # --- TRIGGER GOLD LOAD ---
        load_gold_layer(con, stg_table)

    except Exception as e:
        print(f"⚠️ stg/Gold Error: {e}")
    finally:
        con.close()
def trans_table(spark):
    all_schemas = get_schemas()

    # Danh sách task: (ConfigKey, BronzePrefix, TableName, Func, Schema)
    tasks = [
        ("trades", "bronze/okx_trades/", "trades", transform_trades, all_schemas["trades"]),
        ("funding", "bronze/okx_funding/", "funding_rate", transform_funding, all_schemas["funding"]),
        ("ohlc_mark", "bronze/okx_ohlc_mark/", "ohlc_mark", lambda df: transform_ohlc(df, "mark"), all_schemas["ohlc"]),
        ("ohlc_index", "bronze/okx_ohlc_index/", "ohlc_index", lambda df: transform_ohlc(df, "index"),
         all_schemas["ohlc"]),
        ("book", "bronze/okx_orderbook/", "order_books", transform_orderbook, all_schemas["book"]),
        # Thêm các task khác tương tự...
    ]

    for task in tasks:
        run_pipeline(spark, *task)
#trans_table()