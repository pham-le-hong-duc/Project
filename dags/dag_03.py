# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import timedelta
# from airflow.utils.dates import days_ago
# import threading
# from plugins.spark_config import get_spark
# from ELT.transform.trans_tables_db import trans_table
# from ELT.transform.agg_orderbook import agg_orderbook
# from ELT.transform.agg_trades import agg_trades
# from ELT.transform.index_price_kline import index_price
# from ELT.transform.mark_price_kline import mark_price
# from ELT.transform.merge_features import process_and_merge_all
# from ELT.transform.derives_features import derives_features
#
#
# def run_pipeline():
#     print("🚀 [MASTER] STARTING PIPELINE (ONE SPARK SESSION)...")
#
#     # A. Khởi tạo Spark Session (DUY NHẤT TẠI ĐÂY)
#     spark = get_spark()
#     spark.sparkContext.setLogLevel("ERROR")
#     try:
#         # BƯỚC 1: trans_table (Chạy tuần tự)
#         print("\n🔹 Step 1: Running trans_table...")
#         trans_table(spark)
#
#         # BƯỚC 2: Chạy Song Song (Parallel Group)
#         # agg_orderbook, agg_trades, index_price, mark_price
#         print("\n🔹 Step 2: Running Aggregations in Parallel...")
#         # Định nghĩa các luồng (Thread)
#         t_orderbook = threading.Thread(target=agg_orderbook, args=(spark,))
#         t_trades = threading.Thread(target=agg_trades, args=(spark,))
#         t_index = threading.Thread(target=index_price, args=(spark,))
#         t_mark = threading.Thread(target=mark_price, args=(spark,))
#
#         # Bắt đầu chạy tất cả cùng lúc
#         t_orderbook.start()
#         t_trades.start()
#         t_index.start()
#         t_mark.start()
#
#         # Chờ tất cả các luồng xong mới đi tiếp (Join)
#         t_orderbook.join()
#         t_trades.join()
#         t_index.join()
#         t_mark.join()
#         print("   ✅ All parallel aggregations finished.")
#
#         # BƯỚC 3: process_and_merge_all (Chạy sau khi nhóm trên xong)
#         print("\n🔹 Step 3: Running process_and_merge_all...")
#         process_and_merge_all(spark)
#
#         # BƯỚC 4: derives_features (Chạy cuối cùng)
#         print("\n🔹 Step 4: Running derives_features...")
#         derives_features(spark)
#
#         print("\n🏆 [MASTER] PIPELINE COMPLETED SUCCESSFULLY!")
#
#     except Exception as e:
#         print(f"❌ [MASTER] CRITICAL ERROR: {e}")
#         raise e  # Báo lỗi để Airflow retry
#     finally:
#         # C. Tắt Spark Session (Dù lỗi hay không cũng phải tắt)
#         print("🛑 Stopping Spark Session...")
#         spark.stop()
#
#
# default_args = {
#     'owner': 'trading-data',
#     'retries': 1,
#     'retry_delay': timedelta(minutes=1)
# }
#
# with DAG(
#         'transform_data_silver_and_gold',
#         default_args=default_args,
#         schedule_interval='*/5 * * * *',  # Chạy mỗi 5 phút
#         start_date=days_ago(1),
#         catchup=False,
#         max_active_runs=1
# ) as dag:
#     task_master_run = PythonOperator(
#         task_id='run_pipeline',
#         python_callable=run_pipeline
#     )

''' Khong dung threading'''
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from plugins.spark_config import get_spark
from ELT.transform.trans_tables_db import trans_table
from ELT.transform.agg_orderbook import agg_orderbook
from ELT.transform.agg_trades import agg_trades
from ELT.transform.index_price_kline import index_price
from ELT.transform.mark_price_kline import mark_price
from ELT.transform.merge_features import process_and_merge_all
from ELT.transform.derives_features import derives_features


def run_pipeline():
    print("🚀 [MASTER] STARTING PIPELINE (SEQUENTIAL MODE)...")

    # A. Khởi tạo Spark Session
    # Lưu ý: Spark Session này sẽ sống suốt quá trình chạy Task
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # --- BƯỚC 1: ETL CƠ BẢN (Raw -> Silver -> Gold) ---
        print("\n🔹 Step 1: Running trans_table (ETL Basic)...")
        trans_table(spark)

        # --- BƯỚC 2: AGGREGATIONS (Chạy tuần tự để tiết kiệm RAM) ---
        print("\n🔹 Step 2: Running Aggregations...")

        print("   -> Running agg_orderbook...")
        agg_orderbook(spark)

        print("   -> Running agg_trades...")
        agg_trades(spark)

        print("   -> Running index_price...")
        index_price(spark)

        print("   -> Running mark_price...")
        mark_price(spark)

        # --- BƯỚC 3: MERGE FEATURES ---
        print("\n🔹 Step 3: Running process_and_merge_all...")
        process_and_merge_all(spark)

        # --- BƯỚC 4: DERIVED FEATURES ---
        print("\n🔹 Step 4: Running derives_features...")
        derives_features(spark)

        print("\n🏆 [MASTER] PIPELINE COMPLETED SUCCESSFULLY!")

    except Exception as e:
        print(f"❌ [MASTER] CRITICAL ERROR: {e}")
        # Quan trọng: Raise lỗi để Airflow biết là Task Failed và có thể Retry
        raise e
    finally:
        # C. Luôn tắt Spark Session để giải phóng RAM cho Docker
        print("🛑 Stopping Spark Session...")
        spark.stop()


default_args = {
    'owner': 'trading-data',
    'retries': 1,  # Retry 1 lần nếu lỗi
    'retry_delay': timedelta(minutes=1)
}

with DAG(
        '03_transform_master_dag',  # Đặt tên DAG thống nhất (bắt đầu bằng số thứ tự)
        default_args=default_args,
        schedule_interval='*/5 * * * *',  # Chạy mỗi 5 phút
        start_date=days_ago(1),
        catchup=False,
        max_active_runs=1  # Chỉ cho phép 1 DAG chạy tại 1 thời điểm (tránh chồng chéo)
) as dag:
    task_master_run = PythonOperator(
        task_id='run_pipeline',
        python_callable=run_pipeline
    )