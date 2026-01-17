"""
Perpetual Trades WebSocket Producer using BaseWebSocketStreamProducer.

Architecture: WebSocket → Buffer → Redpanda Topic (okx-perpetual_trades)
"""
import json
import asyncio
import sys
from pathlib import Path

from src.ingestion.common.base.WebSocketStreamProducer import WebSocketStreamProducer, setup_signal_handlers


class PerpetualTradesWebSocketStreamProducer(WebSocketStreamProducer):
    """Perpetual Trades WebSocket Producer."""
    
    def __init__(self, 
                 symbol="BTC-USDT-SWAP",
                 redpanda_bootstrap_servers='redpanda:9092',
                 redpanda_topic='okx-perpetual_trades',
                 buffer_size=128, 
                 buffer_timeout=10):
        super().__init__(
            symbol=symbol,
            data_type="perpetual_trades",
            redpanda_topic=redpanda_topic,
            redpanda_bootstrap_servers=redpanda_bootstrap_servers,
            buffer_size=buffer_size,
            buffer_timeout=buffer_timeout
        )
    
    def get_websocket_url(self):
        return "wss://ws.okx.com:8443/ws/v5/public"
    
    def get_channel(self):
        return "trades"
    
    def get_subscribe_message(self):
        args = [{"channel": self.get_channel(), "instId": self.symbol}]
        return json.dumps({"op": "subscribe", "args": args})
    
    def normalize_message(self, trade):
        """Normalize trade data to standard format."""
        try:
            if isinstance(trade, dict):
                return {
                    "instrument_name": self.symbol,
                    "trade_id": int(trade["tradeId"]),
                    "side": trade["side"],
                    "price": float(trade["px"]),
                    "size": float(trade["sz"]),
                    "created_time": int(trade["ts"])
                }
            return None
        except Exception as e:
            print(f"⚠️  Normalization error: {e}")
            return None


    import os
async def main():
    """Main entry point."""
    stream = PerpetualTradesWebSocketStreamProducer(
        symbol="BTC-USDT-SWAP",
        redpanda_bootstrap_servers='redpanda:9092',
        redpanda_topic='okx-perpetual_trades',
        buffer_size=128,
        buffer_timeout=10
    )
    
    setup_signal_handlers(stream)
    
    print("Start Perpetual Trades WebSocketStream")
    await stream.start()


if __name__ == "__main__":
    asyncio.run(main())
