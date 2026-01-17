"""
Perpetual OrderBook WebSocket Producer using BaseWebSocketStreamProducer.

Architecture: WebSocket → Aggregation State → Buffer → Redpanda Topic (okx-perpetual-orderbook)
Aggregates updates into 1-minute snapshots with rolling state.
"""
import json
import asyncio
import sys
from pathlib import Path
from datetime import datetime, timezone

from src.ingestion.common.base.WebSocketStreamProducer import WebSocketStreamProducer, setup_signal_handlers


class OrderBookWebSocketStreamProducer(WebSocketStreamProducer):
    """Perpetual OrderBook WebSocket Producer with aggregation."""
    
    def __init__(self, 
                 symbol="BTC-USDT-SWAP",
                 redpanda_bootstrap_servers='redpanda:9092',
                 redpanda_topic='okx-perpetual-orderbook',
                 buffer_size=5,
                 buffer_timeout=10):
        super().__init__(
            symbol=symbol,
            data_type="perpetual_orderBook",
            redpanda_topic=redpanda_topic,
            redpanda_bootstrap_servers=redpanda_bootstrap_servers,
            buffer_size=buffer_size,
            buffer_timeout=buffer_timeout
        )
        
        # Aggregation state for 1-minute snapshots
        self._current_minute_ts = None
        self._state_bids = {}  # price -> size
        self._state_asks = {}
        self._last_emitted_minute_ts = None
    
    def get_websocket_url(self):
        return "wss://wspap.okx.com:8443/ws/v5/public"
    
    def get_channel(self):
        return "books"
    
    def get_subscribe_message(self):
        args = [{"channel": self.get_channel(), "instId": self.symbol}]
        return json.dumps({"op": "subscribe", "args": args})
    
    def normalize_message(self, book_update):
        """Not used - orderbook uses custom _handle_message."""
        return None
    
    def _emit_snapshot_from_state(self, inst_id, minute_ts):
        """Emit a snapshot from current aggregation state."""
        try:
            # Build top50 from rolling maps
            bids = [[p, s] for p, s in self._state_bids.items() if s and s > 0]
            asks = [[p, s] for p, s in self._state_asks.items() if s and s > 0]
            
            bids.sort(key=lambda x: x[0], reverse=True)
            asks.sort(key=lambda x: x[0])
            
            asks_l50 = asks[:50]
            bids_l50 = bids[:50]
            
            return {
                "instId": inst_id,
                "action": "snapshot",
                "ts": int(minute_ts),
                "asks": json.dumps(asks_l50),
                "bids": json.dumps(bids_l50)
            }
        except Exception as e:
            print(f"⚠️  Error creating snapshot: {e}")
            return None
    
    def _apply_deltas(self, bids, asks):
        """Apply bid/ask deltas to rolling state."""
        try:
            # Update bids
            for lvl in bids or []:
                if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
                    continue
                try:
                    p = float(lvl[0])
                    s = float(lvl[1])
                    
                    if s <= 0:
                        self._state_bids.pop(p, None)
                    else:
                        self._state_bids[p] = s
                except:
                    continue
            
            # Update asks
            for lvl in asks or []:
                if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
                    continue
                try:
                    p = float(lvl[0])
                    s = float(lvl[1])
                    
                    if s <= 0:
                        self._state_asks.pop(p, None)
                    else:
                        self._state_asks[p] = s
                except:
                    continue
        except Exception as e:
            print(f"⚠️  Error applying deltas: {e}")
    
    def _handle_message(self, message_str):
        """Override to handle orderbook aggregation logic."""
        try:
            data = json.loads(message_str)
            
            if not self.should_process_message(data):
                return
            
            arg = data.get('arg', {})
            inst_id = arg.get('instId')
            
            for book_update in data['data']:
                if not isinstance(book_update, dict):
                    continue
                
                ts = int(book_update.get('ts'))
                minute_ts = (ts // 60000) * 60000
                asks = book_update.get('asks', [])
                bids = book_update.get('bids', [])
                
                # Initialize current minute tracking
                if self._current_minute_ts is None:
                    self._current_minute_ts = minute_ts
                    self._apply_deltas(bids, asks)
                
                # Check if minute changed
                elif minute_ts != self._current_minute_ts:
                    # Emit snapshot for previous minute
                    if self._last_emitted_minute_ts != self._current_minute_ts:
                        snapshot = self._emit_snapshot_from_state(inst_id, self._current_minute_ts)
                        if snapshot:
                            self.buffer.append(snapshot)
                            self._last_emitted_minute_ts = self._current_minute_ts
                            
                            if len(self.buffer) >= self.buffer_size:
                                self._flush_buffer()
                    
                    # Move to new minute
                    self._current_minute_ts = minute_ts
                    self._apply_deltas(bids, asks)
                else:
                    # Same minute, apply deltas
                    self._apply_deltas(bids, asks)
                    
        except Exception as e:
            print(f"⚠️  Message handling error: {e}")


    import os
async def main():
    """Main entry point."""
    stream = OrderBookWebSocketStreamProducer(
        symbol="BTC-USDT-SWAP",
        redpanda_bootstrap_servers='redpanda:9092',
        redpanda_topic='okx-perpetual-orderbook',
        buffer_size=5,
        buffer_timeout=10
    )
    
    setup_signal_handlers(stream)
    
    print("Start Perpetual OrderBook WebSocketStream")
    await stream.start()


if __name__ == "__main__":
    asyncio.run(main())
