"""
Base Download class for simple data downloads (trades & fundingRate).

This class contains common logic for downloading ZIP files from OKX and converting to Parquet.
Improvements over original:
1. No HEAD pre-check (faster, MinIO check is sufficient)
2. BytesIO in-memory processing (faster, no disk I/O)
3. Signal handler in __init__ (cleaner, automatic setup)
"""
import sys
import signal
import zipfile
import requests
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
from io import BytesIO
from abc import ABC, abstractmethod

from src.utils.s3_client import MinIOWriter


class Download(ABC):
    """
    Base class for downloading historical data from OKX and storing in MinIO.
    
    Subclasses must implement 2 abstract methods to define data-specific behavior:
    - get_url_format_params()
    - get_date_iterator()
    """
    
    def __init__(self,
                 symbol,
                 data_type,
                 url_template,
                 base_start_date="2025-01-01",
                 minio_endpoint=None,
                 minio_access_key=None,
                 minio_secret_key=None,
                 minio_bucket="okx"):
        """
        Initialize base download handler.
        
        Args:
            symbol: Trading symbol (e.g., "BTC-USDT", "BTC-USDT-SWAP")
            data_type: Type of data (e.g., "spot_trades", "perpetual_fundingRate")
            url_template: URL template with placeholders
            base_start_date: Starting date for download
            minio_endpoint: MinIO endpoint URL
            minio_access_key: MinIO access key
            minio_secret_key: MinIO secret key
            minio_bucket: MinIO bucket name
        """
        self.symbol = symbol
        self.data_type = data_type
        self.base_start_date = base_start_date
        self.url_template = url_template
        
        # Initialize MinIO writer
        self.minio_writer = MinIOWriter(
            endpoint=minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            bucket=minio_bucket,
            secure=False
        )
        print(f"Using MinIO storage: s3://{minio_bucket or 'okx'}/{self.data_type}/{symbol.lower()}/")
        
        self.interrupted = False
        
        # Setup signal handlers (IMPROVEMENT 3: automatic setup)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle interrupt signals for graceful shutdown."""
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        self.interrupted = True
    
    # ==================== Abstract Methods (must be implemented by subclasses) ====================
    
    @abstractmethod
    def get_url_format_params(self, date_obj):
        """
        Return URL formatting parameters for a specific date.
        
        Args:
            date_obj: datetime object
        
        Returns:
            dict: Parameters to format url_template
                  e.g., {"YYYYMMDD": "20250112", "SYMBOL": "BTC-USDT", "YYYY_MM_DD": "2025-01-12"}
        """
        pass
    
    @abstractmethod
    def get_date_iterator(self, start_date, end_date):
        """
        Return an iterator over dates to download.
        
        Args:
            start_date: Start datetime
            end_date: End datetime
        
        Yields:
            tuple: (date_obj, period_str)
                   e.g., (datetime(2025, 1, 12), "2025-01-12") for daily
                         (datetime(2025, 1, 1), "2025-01") for monthly
        """
        pass
    
    # ==================== Common Download Logic ====================
    
    def _get_existing_files(self):
        """Get set of existing filenames in MinIO."""
        try:
            prefix = f"{self.data_type}/{self.symbol.lower()}/"
            objects = self.minio_writer.client.list_objects(
                self.minio_writer.bucket,
                prefix=prefix,
                recursive=False
            )
            
            existing = set()
            for obj in objects:
                filename = obj.object_name.split('/')[-1]
                if filename.endswith('.parquet'):
                    existing.add(filename)
            
            return existing
        except Exception as e:
            print(f"Error listing existing files: {e}")
            return set()
    
    def _download_and_convert(self, url, period_str):
        """
        Download and convert files using BytesIO (IMPROVEMENT 2: in-memory processing).
        
        Args:
            url: URL to download
            period_str: Period string for filename (e.g., "2025-01-12" or "2025-01")
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Download ZIP file
            response = requests.get(url, timeout=30)
            
            if response.status_code != 200:
                return False
            
            # Extract CSV from ZIP (IMPROVEMENT 2: in-memory with BytesIO)
            with zipfile.ZipFile(BytesIO(response.content)) as zf:
                csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
                if not csv_files:
                    return False
                
                # Read CSV directly from ZIP
                with zf.open(csv_files[0]) as csv_file:
                    df = pl.read_csv(csv_file)
                    
                    if df.is_empty():
                        return False
            
            # Write to MinIO
            object_path = f"{self.data_type}/{self.symbol.lower()}/{period_str}.parquet"
            if self.minio_writer.write_parquet(df, object_path):
                return True
            else:
                return False
                
        except Exception as e:
            return False
    
    # ==================== Main Run Method ====================
    
    def run(self):
        """
        Main run method: Download all missing files from start_date to today.
        IMPROVEMENT 1: No HEAD pre-check, just try to download and handle failures.
        """
        # Get existing files from MinIO
        existing_files = self._get_existing_files()
        print(f"Found {len(existing_files)} existing files in MinIO")
        
        # Parse start date
        start_date = datetime.strptime(self.base_start_date, "%Y-%m-%d")
        end_date = datetime.now()
        
        # Determine message based on data type
        if "fundingRate" in self.data_type:
            file_type = "monthly"
        else:
            file_type = "daily"
        
        print(f"Downloading missing {file_type} files to MinIO...\n")
        
        # Iterate through dates and download missing files
        downloaded_count = 0
        skipped_count = 0
        failed_count = 0
        
        for date_obj, period_str in self.get_date_iterator(start_date, end_date):
            # Check for interruption
            if self.interrupted:
                print("\n⚠️  Download interrupted")
                break
            
            # Check if file already exists (IMPROVEMENT 1: no HEAD pre-check)
            filename = f"{period_str}.parquet"
            if filename in existing_files:
                skipped_count += 1
                continue
            
            # Build URL
            url_params = self.get_url_format_params(date_obj)
            url = self.url_template.format(**url_params)
            
            # Download and convert
            if self._download_and_convert(url, period_str):
                print(f"✓ {filename} → MinIO")
                downloaded_count += 1
            else:
                print(f"✗ {filename}")
                failed_count += 1
        
        # Print summary
        print(f"\nDownloaded: {downloaded_count}, Skipped: {skipped_count}, Failed: {failed_count}")
