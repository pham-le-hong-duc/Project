"""
Base WebSocket Stream Producer for OKX Data Ingestion.

This base class contains common logic for streaming WebSocket data to Redpanda.
Reduces code duplication across different data types.

Subclasses must implement:
- get_websocket_url()
- get_channel()
- get_subscribe_message()
- normalize_message(data)
"""
import json
import asyncio
import signal
import platform
import sys
from pathlib import Path
from datetime import datetime, timezone
from abc import ABC, abstractmethod
import websockets

from src.ingestion.common.redpanda.Producer import Producer


class WebSocketStreamProducer(ABC):
    """
    Base class for WebSocket stream producers.
    
    Architecture: WebSocket → Buffer → Redpanda Topic
    """
    
    def __init__(self,
                 symbol,
                 data_type,
                 redpanda_topic,
                 redpanda_bootstrap_servers='localhost:19092',
                 buffer_size=100,
                 buffer_timeout=60):
        """
        Initialize WebSocket stream producer.
        
        Args:
            symbol: Trading symbol (e.g., "BTC-USDT", "BTC-USDT-SWAP")
            data_type: Data type identifier
            redpanda_topic: Redpanda topic to send messages to
            redpanda_bootstrap_servers: Redpanda broker addresses
            buffer_size: Number of messages to buffer before sending
            buffer_timeout: Seconds before auto-flushing buffer
        """
        self.symbol = symbol
        self.data_type = data_type
        self.redpanda_topic = redpanda_topic
        
        # Initialize Redpanda producer
        try:
            self.producer = Producer(
                bootstrap_servers=redpanda_bootstrap_servers,
                topic=redpanda_topic
            )
            print(f"✅ Streaming to Redpanda topic: {redpanda_topic}")
        except Exception as e:
            print(f"❌ Failed to initialize Redpanda producer: {e}")
            raise
        
        self.running = False
        self.buffer = []
        self.buffer_size = buffer_size
        self.buffer_timeout = buffer_timeout
        self.last_flush_time = datetime.now(timezone.utc)
    
    # ==================== Abstract Methods ====================
    
    @abstractmethod
    def get_websocket_url(self):
        """
        Return WebSocket URL to connect to.
        
        Returns:
            str: WebSocket URL
        """
        pass
    
    @abstractmethod
    def get_channel(self):
        """
        Return channel name for subscription.
        
        Returns:
            str: Channel name (e.g., "trades", "books")
        """
        pass
    
    @abstractmethod
    def get_subscribe_message(self):
        """
        Return subscribe message to send to WebSocket.
        
        Returns:
            str: JSON string of subscribe message
        """
        pass
    
    @abstractmethod
    def normalize_message(self, data):
        """
        Normalize a single message from WebSocket to standard format.
        
        Args:
            data: Raw data from WebSocket
        
        Returns:
            dict: Normalized data, or None to skip
        """
        pass
    
    # ==================== Optional Hooks ====================
    
    def should_process_message(self, message_data):
        """
        Optional hook to filter messages before processing.
        
        Args:
            message_data: Parsed JSON message
        
        Returns:
            bool: True to process, False to skip
        """
        return (message_data.get('arg', {}).get('channel') == self.get_channel() 
                and 'data' in message_data)
    
    def on_flush_success(self, count):
        """Optional hook called after successful flush."""
        print(f"✓ {count} messages → {self.redpanda_topic}")
    
    def on_flush_error(self, error):
        """Optional hook called on flush error."""
        print(f"❌ Error flushing buffer to Redpanda: {error}")
    
    # ==================== Common Logic ====================
    
    def _handle_message(self, message_str):
        """Handle incoming WebSocket message."""
        try:
            data = json.loads(message_str)
            
            if self.should_process_message(data):
                for item in data['data']:
                    normalized = self.normalize_message(item)
                    
                    if normalized:
                        self.buffer.append(normalized)
                        
                        if len(self.buffer) >= self.buffer_size:
                            self._flush_buffer()
                            
        except Exception as e:
            print(f"⚠️  Message handling error: {e}")
    
    def _flush_buffer(self):
        """Flush buffer to Redpanda."""
        if not self.buffer:
            return
        
        try:
            count = len(self.buffer)
            sent_count = self.producer.send_batch(self.buffer)
            
            # Only report success if messages were actually queued
            if sent_count > 0:
                self.producer.flush(timeout=30)  # Increased timeout
                self.on_flush_success(sent_count)
            else:
                print(f"⚠️  No messages were queued to Redpanda")
            
            self.buffer.clear()
            self.last_flush_time = datetime.now(timezone.utc)
            
        except Exception as e:
            self.on_flush_error(e)
    
    def _should_flush(self):
        """Check if buffer should be flushed based on timeout."""
        elapsed = (datetime.now(timezone.utc) - self.last_flush_time).total_seconds()
        return elapsed >= self.buffer_timeout
    
    async def _timeout_checker(self):
        """Background task to check for buffer timeout and auto-flush."""
        while self.running:
            try:
                if self._should_flush() and self.buffer:
                    self._flush_buffer()
                
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"⚠️  Timeout checker error: {e}")
    
    async def start(self):
        """Start WebSocket stream and produce to Redpanda."""
        self.running = True
        timeout_task = None
        
        try:
            timeout_task = asyncio.create_task(self._timeout_checker())
            
            async with websockets.connect(self.get_websocket_url()) as websocket:
                print("Connected")
                await websocket.send(self.get_subscribe_message())
                print(f"Subscribed to {self.get_channel()} for {self.symbol}")
                
                async for message in websocket:
                    if not self.running:
                        break
                    
                    self._handle_message(message)
        
        except websockets.exceptions.ConnectionClosed:
            print("Connection closed")
        except Exception as e:
            print(f"❌ Error: {e}")
        finally:
            self.running = False
            
            if timeout_task:
                timeout_task.cancel()
                try:
                    await timeout_task
                except asyncio.CancelledError:
                    pass
            
            if self.buffer:
                self._flush_buffer()
            
            self.producer.close()
            print("Stopped")
    
    def stop(self):
        """Stop the WebSocket stream."""
        self.running = False


# ==================== Helper Function for Signal Handling ====================

def setup_signal_handlers(stream_instance):
    """
    Setup signal handlers for graceful shutdown.
    
    Args:
        stream_instance: WebSocket stream instance to stop on signal
    """
    def signal_handler(sig, frame):
        print("\nReceived interrupt signal, stopping...")
        if stream_instance:
            stream_instance.stop()
    
    if platform.system() == 'Windows':
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGBREAK, signal_handler)
    else:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
