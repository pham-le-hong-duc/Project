"""Refactored Index Price Klines RestAPI using BaseRestAPI."""
import asyncio
import sys
from pathlib import Path
import okx.api.public as PublicData

from src.ingestion.common.base.RestAPI import RestAPI


class IndexPriceKlinesRestAPI(RestAPI):
    """Index Price Klines REST API - Refactored version."""
    
    def __init__(self, 
                 symbol="BTC-USDT",
                 base_start_date="2025-01-01",
                 minio_endpoint=None,
                 minio_access_key=None,
                 minio_secret_key=None,
                 minio_bucket="okx",
                 buffer_size=2000):
        super().__init__(
            symbol=symbol,
            data_type="indexPriceKlines",
            base_start_date=base_start_date,
            minio_endpoint=minio_endpoint,
            minio_access_key=minio_access_key,
            minio_secret_key=minio_secret_key,
            minio_bucket=minio_bucket,
            buffer_size=buffer_size
        )
        self.publicAPI = PublicData.Public(flag="0")
        self.interval = "1m"
        self.gap_threshold = int(1.5 * 60 * 1000)  # 1.5 minutes
    
    def get_timestamp_field(self):
        return "open_time"
    
    def get_unique_field(self):
        return "open_time"
    
    def get_file_pattern(self):
        return "daily"
    
    async def fetch_data_from_api(self, start_ms, end_ms, current_after, **kwargs):
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            self.publicAPI.get_history_index_candles,
            self.symbol,
            current_after,
            "",
            self.interval,
            "100"
        )
        
        if result.get('code') != '0' or not result.get('data'):
            return []
        
        return result['data']
    
    def transform_api_response(self, item):
        return {
            'open_time': int(item[0]),
            'open': float(item[1]),
            'high': float(item[2]),
            'low': float(item[3]),
            'close': float(item[4])
        }
    
    def _extract_timestamp(self, item):
        return int(item[0])
    
    async def run(self):
        gaps = self.detect_gaps(gap_threshold=self.gap_threshold)
        self.show_gaps(gaps)
        
        if gaps:
            await self.fill_gaps(gaps)


if __name__ == "__main__":
    try:
        api = IndexPriceKlinesRestAPI(
            symbol="BTC-USDT",
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
