"""
Perpetual Mark Price Klines WebSocket Producer using BaseWebSocketStreamProducer.

Architecture: WebSocket → Buffer → Redpanda Topic (okx-perpetual_markPriceKlines)
Only 1m interval.
"""
import json
import asyncio
import sys
from pathlib import Path

from src.ingestion.common.base.WebSocketStreamProducer import WebSocketStreamProducer, setup_signal_handlers


class MarkPriceKlinesWebSocketStreamProducer(WebSocketStreamProducer):
    """Perpetual Mark Price Klines WebSocket Producer."""
    
    def __init__(self, 
                 symbol="BTC-USDT-SWAP",
                 redpanda_bootstrap_servers='redpanda:9092',
                 redpanda_topic='okx-perpetual_markPriceKlines',
                 buffer_size=1, 
                 buffer_timeout=60):
        super().__init__(
            symbol=symbol,
            data_type="perpetual_markPriceKlines",
            redpanda_topic=redpanda_topic,
            redpanda_bootstrap_servers=redpanda_bootstrap_servers,
            buffer_size=buffer_size,
            buffer_timeout=buffer_timeout
        )
        
        # Only support 1m interval
        self.channel_name = "mark-price-candle1m"
        
        # Track processed open_time to avoid duplicates
        self.processed_open_times = set()
    
    def get_websocket_url(self):
        return "wss://wspap.okx.com:8443/ws/v5/business"
    
    def get_channel(self):
        return self.channel_name
    
    def get_subscribe_message(self):
        args = [{"channel": self.channel_name, "instId": self.symbol}]
        return json.dumps({"op": "subscribe", "args": args})
    
    def normalize_message(self, kline_data):
        """Normalize kline data with deduplication check."""
        try:
            if isinstance(kline_data, list) and len(kline_data) >= 5:
                open_time = int(kline_data[0])
                
                # Check for duplicates
                if open_time in self.processed_open_times:
                    return None
                
                self.processed_open_times.add(open_time)
                
                return {
                    "open_time": open_time,
                    "open": float(kline_data[1]),
                    "high": float(kline_data[2]),
                    "low": float(kline_data[3]),
                    "close": float(kline_data[4])
                }
            return None
        except Exception as e:
            print(f"⚠️  Normalization error: {e}")
            return None


    import os
async def main():
    """Main entry point."""
    stream = MarkPriceKlinesWebSocketStreamProducer(
        symbol="BTC-USDT-SWAP",
        redpanda_bootstrap_servers='redpanda:9092',
        redpanda_topic='okx-perpetual_markPriceKlines',
        buffer_size=1,
        buffer_timeout=60
    )
    
    setup_signal_handlers(stream)
    
    print("Start Perpetual Mark Price Klines WebSocketStream")
    await stream.start()


if __name__ == "__main__":
    asyncio.run(main())
