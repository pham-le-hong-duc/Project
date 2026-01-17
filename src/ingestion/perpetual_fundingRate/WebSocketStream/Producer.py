"""
Perpetual Funding Rate WebSocket Producer using BaseWebSocketStreamProducer.

Architecture: WebSocket → Buffer → Redpanda Topic (okx-perpetual_fundingRate)
Updates every 8 hours.
"""
import json
import asyncio
import sys
from pathlib import Path

from src.ingestion.common.base.WebSocketStreamProducer import WebSocketStreamProducer, setup_signal_handlers


class FundingRateWebSocketStreamProducer(WebSocketStreamProducer):
    """Perpetual Funding Rate WebSocket Producer."""
    
    def __init__(self, 
                 symbol="BTC-USDT-SWAP",
                 redpanda_bootstrap_servers='redpanda:9092',
                 redpanda_topic='okx-perpetual_fundingRate',
                 buffer_size=1,
                 buffer_timeout=300):
        super().__init__(
            symbol=symbol,
            data_type="perpetual_fundingRate",
            redpanda_topic=redpanda_topic,
            redpanda_bootstrap_servers=redpanda_bootstrap_servers,
            buffer_size=buffer_size,
            buffer_timeout=buffer_timeout
        )
        
        # Track processed funding times to avoid duplicates
        self.processed_funding_times = set()
    
    def get_websocket_url(self):
        return "wss://wspap.okx.com:8443/ws/v5/public"
    
    def get_channel(self):
        return "funding-rate"
    
    def get_subscribe_message(self):
        args = [{"channel": self.get_channel(), "instId": self.symbol}]
        return json.dumps({"op": "subscribe", "args": args})
    
    def normalize_message(self, funding_data):
        """Normalize funding rate data with deduplication check."""
        try:
            if isinstance(funding_data, dict):
                funding_time = int(funding_data["fundingTime"])
                
                # Check for duplicates
                if funding_time in self.processed_funding_times:
                    return None
                
                self.processed_funding_times.add(funding_time)
                
                return {
                    "instrument_name": self.symbol,
                    "funding_rate": float(funding_data["fundingRate"]),
                    "funding_time": funding_time
                }
            return None
        except Exception as e:
            print(f"⚠️  Normalization error: {e}")
            return None


    import os
async def main():
    """Main entry point."""
    stream = FundingRateWebSocketStreamProducer(
        symbol="BTC-USDT-SWAP",
        redpanda_bootstrap_servers='redpanda:9092',
        redpanda_topic='okx-perpetual_fundingRate',
        buffer_size=1,
        buffer_timeout=300
    )
    
    setup_signal_handlers(stream)
    
    print("Start Perpetual Funding Rate WebSocketStream")
    await stream.start()


if __name__ == "__main__":
    asyncio.run(main())
