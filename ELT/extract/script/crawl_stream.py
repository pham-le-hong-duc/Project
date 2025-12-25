# File: plugins/spark_jobs/kafka_collector.py

import asyncio
import json
import logging
import os
import signal
from datetime import datetime
import websockets
from confluent_kafka import Producer

# Setup Logging
logger = logging.getLogger("airflow.task")  # Dùng logger của Airflow

# Các hằng số
OKX_WS_PUB = "wss://ws.okx.com:8443/ws/v5/public"
OKX_WS_BUSINESS = "wss://ws.okx.com:8443/ws/v5/business"

# Topics
TOPIC_TRADES = "okx_trades"
TOPIC_ORDERBOOK = "okx_orderbook"
TOPIC_FUNDING = "okx_funding"
TOPIC_CANDLE_MARK = "okx_ohlc_mark"
TOPIC_CANDLE_INDEX = "okx_ohlc_index"

# Symbols
SYMBOL_SWAP = "BTC-USDT-SWAP"
SYMBOL_INDEX = "BTC-USDT"

# Global Flag
running = True


def get_producer(bootstrap_servers):
    config = {
        "bootstrap.servers": bootstrap_servers,
        "linger.ms": 50,
        "batch.size": 16384,
        "compression.type": "gzip"
    }
    return Producer(config)


def delivery_callback(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    # Giảm log success để tránh spam log Airflow
    # else:
    #     logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')


async def route_message(raw_msg, producer):
    if raw_msg == "pong": return

    try:
        msg = json.loads(raw_msg)
    except json.JSONDecodeError:
        return

    if 'event' in msg:
        if msg['event'] == 'error':
            logger.error(f"OKX Error: {msg}")
        return

    arg = msg.get("arg", {})
    chan = arg.get("channel", "")

    # Routing Logic
    if chan == "trades":
        topic = TOPIC_TRADES
    elif "book" in chan:
        topic = TOPIC_ORDERBOOK
    elif chan == "funding-rate":
        topic = TOPIC_FUNDING
    elif "mark-price-candle" in chan:
        topic = TOPIC_CANDLE_MARK
    elif "index-candle" in chan:
        topic = TOPIC_CANDLE_INDEX
    else:
        topic = "okx_others"

    record = {
        "received_at": datetime.now().isoformat(),
        "payload": msg
    }

    try:
        producer.produce(topic, json.dumps(record).encode("utf-8"), on_delivery=delivery_callback)
        producer.poll(0)
    except BufferError:
        producer.poll(1)
        producer.produce(topic, json.dumps(record).encode("utf-8"), on_delivery=delivery_callback)


async def heartbeat(ws):
    while running:
        try:
            await ws.send("ping")
            await asyncio.sleep(20)
        except Exception:
            break


async def run_collector(url, channels, producer, socket_name='WS'):
    global running
    while running:
        try:
            async with websockets.connect(url, ping_interval=None) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": channels}))
                logger.info(f"Subscribed: {socket_name}")

                hb = asyncio.create_task(heartbeat(ws))

                while running:
                    try:
                        # Thêm timeout để cho phép loop kiểm tra biến running
                        msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        await route_message(msg, producer)
                    except asyncio.TimeoutError:
                        continue  # Tiếp tục vòng lặp để check running
                    except websockets.ConnectionClosed:
                        break
                hb.cancel()
        except Exception as e:
            if running:
                logger.error(f"[{socket_name}] Error: {e}. Reconnecting...")
                await asyncio.sleep(3)


async def main_async(bootstrap_servers, runtime_seconds):
    global running
    running = True
    producer = get_producer(bootstrap_servers)

    # Prepare Subscription Args
    sub_arg_pub = [
        {"channel": "trades", "instId": SYMBOL_SWAP},
        {"channel": "books", "instId": SYMBOL_INDEX},
        {"channel": "funding-rate", "instId": SYMBOL_SWAP},
    ]

    sub_arg_business = []
    for tf in ["1m", "5m", "15m", "1H"]:
        sub_arg_business.append({"channel": f"mark-price-candle{tf}", "instId": SYMBOL_SWAP})
        sub_arg_business.append({"channel": f"index-candle{tf}", "instId": SYMBOL_INDEX})

    # Run Collectors
    task_pub = asyncio.create_task(run_collector(OKX_WS_PUB, sub_arg_pub, producer, 'PUB_WS'))
    task_biz = asyncio.create_task(run_collector(OKX_WS_BUSINESS, sub_arg_business, producer, 'BIZ_WS'))

    logger.info(f"🚀 Collector started. Will run for {runtime_seconds} seconds.")

    # Chạy trong khoảng thời gian quy định
    await asyncio.sleep(runtime_seconds)

    # Graceful Shutdown
    logger.info("⏳ Time limit reached. Shutting down...")
    running = False

    # Chờ tasks dừng hẳn
    await asyncio.gather(task_pub, task_biz, return_exceptions=True)

    # Flush Kafka
    producer.flush(timeout=5)
    logger.info("✅ Producer flushed and shutdown complete.")


# --- WRAPPER CHO AIRFLOW ---
def run_kafka_collector(kafka_bootstrap="redpanda:29092", runtime_seconds=3500):
    """
    Hàm này sẽ được gọi bởi Airflow PythonOperator.
    runtime_seconds mặc định là 3500s (~58 phút) để khớp với schedule 1 tiếng.
    """
    try:
        asyncio.run(main_async(kafka_bootstrap, runtime_seconds))
    except Exception as e:
        logger.error(f"Critical Error in Collector: {e}")
        raise e