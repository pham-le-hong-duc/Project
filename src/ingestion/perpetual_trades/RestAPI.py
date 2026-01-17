"""Refactored Perpetual Trades RestAPI using BaseRestAPI."""
import asyncio
import sys
import polars as pl
from pathlib import Path
import okx.api.market as MarketData

from src.ingestion.common.base.RestAPI import RestAPI


class PerpetualTradesRestAPI(RestAPI):
    """Perpetual Trades REST API - Refactored version."""
    
    def __init__(self, 
                 symbol="BTC-USDT-SWAP",
                 base_start_date="2025-01-01",
                 minio_endpoint=None,
                 minio_access_key=None,
                 minio_secret_key=None,
                 minio_bucket="okx",
                 buffer_size=2000):
        super().__init__(
            symbol=symbol,
            data_type="perpetual_trades",
            base_start_date=base_start_date,
            minio_endpoint=minio_endpoint,
            minio_access_key=minio_access_key,
            minio_secret_key=minio_secret_key,
            minio_bucket=minio_bucket,
            buffer_size=buffer_size
        )
        self.marketAPI = MarketData.Market()
    
    def get_timestamp_field(self):
        return "created_time"
    
    def get_unique_field(self):
        return "trade_id"
    
    def get_file_pattern(self):
        return "daily"
    
    async def fetch_data_from_api(self, start_ms, end_ms, current_after, **kwargs):
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, 
            self.marketAPI.get_history_trades,
            self.symbol,
            "2",
            current_after,
            "",
            "100"
        )
        
        if result.get('code') != '0' or not result.get('data'):
            return []
        
        return result['data']
    
    def transform_api_response(self, item):
        return {
            'instrument_name': self.symbol,
            'trade_id': int(item['tradeId']),
            'side': item['side'],
            'price': float(item['px']),
            'size': float(item['sz']),
            'created_time': int(item['ts'])
        }
    
    def _extract_timestamp(self, item):
        return int(item['ts'])
    
    async def run(self):
        gap_threshold = 60000  # 1 minute (60 seconds)
        gaps = self.detect_gaps(gap_threshold=gap_threshold)
        self.show_gaps(gaps)
        
        if gaps:
            await self.fill_gaps(gaps)


if __name__ == "__main__":
    try:
        api = PerpetualTradesRestAPI(
            symbol="BTC-USDT-SWAP",
            base_start_date="2025-01-01",
            minio_endpoint=None,
            minio_access_key=None,
            minio_secret_key=None,
            minio_bucket="okx",
            buffer_size=2000
        )
        asyncio.run(api.run())
        
    except KeyboardInterrupt:
        print("\nProcess interrupted by user (Ctrl+C)")
        print("Exiting gracefully...")
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
