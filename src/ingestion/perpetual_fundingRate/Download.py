"""Refactored Funding Rate Download using BaseDownload with improvements."""
import sys
from pathlib import Path

from src.ingestion.common.base.Download import Download


class PerpetualFundingRateDownload(Download):
    """Funding Rate Download - Refactored version with improvements."""
    
    def __init__(self,
                 symbol="BTC-USDT-SWAP",
                 base_start_date="2025-01-01",
                 minio_endpoint=None,
                 minio_access_key=None,
                 minio_secret_key=None,
                 minio_bucket="okx"):
        url_template = "https://static.okx.com/cdn/okex/traderecords/swaprates/monthly/{YYYYMM}/{SYMBOL}-fundingrates-{YYYY_MM}.zip"
        super().__init__(
            symbol=symbol,
            data_type="perpetual_fundingRate",
            url_template=url_template,
            base_start_date=base_start_date,
            minio_endpoint=minio_endpoint,
            minio_access_key=minio_access_key,
            minio_secret_key=minio_secret_key,
            minio_bucket=minio_bucket
        )
    
    def get_url_format_params(self, date_obj):
        return {
            "YYYYMM": date_obj.strftime("%Y%m"),
            "SYMBOL": self.symbol,
            "YYYY_MM": date_obj.strftime("%Y-%m")
        }
    
    def get_date_iterator(self, start_date, end_date):
        current = start_date.replace(day=1)
        while current <= end_date:
            yield (current, current.strftime("%Y-%m"))
            # Move to next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)


if __name__ == "__main__":
    downloader = PerpetualFundingRateDownload()
    downloader.run()
