"""Refactored Perpetual Trades Download using BaseDownload with improvements."""
import sys
from pathlib import Path
from datetime import timedelta

from src.ingestion.common.base.Download import Download


class PerpetualTradesDownload(Download):
    """Perpetual Trades Download - Refactored version with improvements."""
    
    def __init__(self,
                 symbol="BTC-USDT-SWAP",
                 base_start_date="2025-01-01",
                 minio_endpoint=None,
                 minio_access_key=None,
                 minio_secret_key=None,
                 minio_bucket="okx"):
        url_template = "https://static.okx.com/cdn/okex/traderecords/trades/daily/{YYYYMMDD}/{SYMBOL}-trades-{YYYY_MM_DD}.zip"
        super().__init__(
            symbol=symbol,
            data_type="perpetual_trades",
            url_template=url_template,
            base_start_date=base_start_date,
            minio_endpoint=minio_endpoint,
            minio_access_key=minio_access_key,
            minio_secret_key=minio_secret_key,
            minio_bucket=minio_bucket
        )
    
    def get_url_format_params(self, date_obj):
        return {
            "YYYYMMDD": date_obj.strftime("%Y%m%d"),
            "SYMBOL": self.symbol,
            "YYYY_MM_DD": date_obj.strftime("%Y-%m-%d")
        }
    
    def get_date_iterator(self, start_date, end_date):
        current = start_date
        while current <= end_date:
            yield (current, current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)


if __name__ == "__main__":
    downloader = PerpetualTradesDownload(base_start_date="2025-01-01")
    downloader.run()
