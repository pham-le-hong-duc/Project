"""
Base RestAPI class for all data ingestion modules.

This class contains common logic for gap detection and filling across all data types.
Reduces code duplication from ~1,700 lines across 5 files to ~400 lines in base class.

Key differences handled by subclasses (via abstract methods):
1. Timestamp field: 'created_time' (trades) vs 'open_time' (klines) vs 'funding_time' (fundingRate)
2. Unique field: 'trade_id' (trades) vs 'open_time' (klines) vs 'funding_time' (fundingRate)
3. File pattern: 'daily' (trades/klines) vs 'monthly' (fundingRate)
4. API call & data transformation: Different API methods and response schemas
"""
import asyncio
import sys
import polars as pl
from pathlib import Path
from datetime import datetime, timezone
from abc import ABC, abstractmethod

from src.utils.s3_client import MinIOWriter


class RestAPI(ABC):
    """
    Base class for REST API data ingestion with gap detection and filling capabilities.
    
    Subclasses must implement 6 abstract methods to define data-specific behavior:
    - get_timestamp_field()
    - get_unique_field()
    - get_file_pattern()
    - fetch_data_from_api()
    - transform_api_response()
    - _extract_timestamp()
    """
    
    def __init__(self, 
                 symbol,
                 data_type,
                 base_start_date="2025-01-01",
                 minio_endpoint=None,
                 minio_access_key=None,
                 minio_secret_key=None,
                 minio_bucket="okx",
                 buffer_size=2000):
        """
        Initialize base REST API handler.
        
        Args:
            symbol: Trading symbol (e.g., "BTC-USDT", "BTC-USDT-SWAP")
            data_type: Type of data (e.g., "spot_trades", "indexPriceKlines", "perpetual_fundingRate")
            base_start_date: Starting date for data collection
            minio_endpoint: MinIO endpoint URL
            minio_access_key: MinIO access key
            minio_secret_key: MinIO secret key
            minio_bucket: MinIO bucket name
            buffer_size: Buffer size for batch processing
        """
        self.symbol = symbol
        self.data_type = data_type
        self.base_start_date = base_start_date
        self.buffer_size = buffer_size
        
        # Initialize MinIO writer
        self.minio_writer = MinIOWriter(
            endpoint=minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            bucket=minio_bucket,
            secure=False
        )
        print(f"Using MinIO storage: s3://{minio_bucket or 'okx'}/{self.data_type}/{symbol.lower()}/")
    
    # ==================== Abstract Methods (must be implemented by subclasses) ====================
    
    @abstractmethod
    def get_timestamp_field(self):
        """
        Return the name of the timestamp field for this data type.
        
        Returns:
            str: Field name - 'created_time', 'open_time', or 'funding_time'
        """
        pass
    
    @abstractmethod
    def get_unique_field(self):
        """
        Return the name of the unique identifier field for this data type.
        
        Returns:
            str: Field name - 'trade_id', 'open_time', or 'funding_time'
        """
        pass
    
    @abstractmethod
    def get_file_pattern(self):
        """
        Return the file naming pattern for this data type.
        
        Returns:
            str: 'daily' for trades/klines (2025-01-12.parquet)
                 'monthly' for fundingRate (2025-01.parquet)
        """
        pass
    
    @abstractmethod
    async def fetch_data_from_api(self, start_ms, end_ms, current_after, **kwargs):
        """
        Fetch data from API for the given time range.
        
        Args:
            start_ms: Start timestamp in milliseconds
            end_ms: End timestamp in milliseconds
            current_after: Pagination cursor (timestamp string)
            **kwargs: Additional parameters (e.g., interval='1m' for klines)
        
        Returns:
            List of data records from API, or empty list if no data/error
        """
        pass
    
    @abstractmethod
    def transform_api_response(self, item):
        """
        Transform API response into standardized format.
        
        Args:
            item: Single item from API response
        
        Returns:
            dict: Standardized record with required fields
        """
        pass
    
    @abstractmethod
    def _extract_timestamp(self, item):
        """
        Extract timestamp from API response item.
        
        Args:
            item: Single item from API response
        
        Returns:
            int: Timestamp in milliseconds
        """
        pass
    
    # ==================== Common Gap Detection Methods ====================
    
    def detect_gaps(self, gap_threshold, **kwargs):
        """
        Detect gaps in data stored in MinIO.
        
        This method scans all parquet files in MinIO and detects two types of gaps:
        1. Boundary gaps: Gaps between consecutive files
        2. Internal gaps: Gaps within a single file
        
        Args:
            gap_threshold: Threshold for gap detection in milliseconds
            **kwargs: Additional parameters (e.g., interval for klines)
        
        Returns:
            List of gap dictionaries with structure:
            {
                'type': 'boundary' or 'internal',
                'start': start_timestamp_ms,
                'end': end_timestamp_ms,
                'duration_hours': gap_duration_in_hours,
                'between_files': 'file1.parquet → file2.parquet' (for boundary gaps),
                'inside_file': 'file.parquet' (for internal gaps)
            }
        """
        try:
            # List all files in MinIO for this data type and symbol
            prefix = f"{self.data_type}/{self.symbol.lower()}/"
            
            objects = self.minio_writer.client.list_objects(
                self.minio_writer.bucket,
                prefix=prefix,
                recursive=False
            )
            
            # Get all parquet files and sort by date
            file_dates = []
            for obj in objects:
                filename = obj.object_name.split('/')[-1]
                if filename.endswith('.parquet'):
                    # Extract date from filename
                    # e.g., "2025-01-12.parquet" (daily) or "2025-01.parquet" (monthly)
                    date_str = filename.replace('.parquet', '')
                    file_dates.append((date_str, obj.object_name))
            
            if not file_dates:
                print("No files found in MinIO")
                return []
            
            # Sort by date
            file_dates.sort(key=lambda x: x[0])
            
            print(f"Found {len(file_dates)} files in MinIO")
            
            # Detect both types of gaps
            gaps = []
            gaps.extend(self._detect_boundary_gaps_minio(file_dates, gap_threshold))
            gaps.extend(self._detect_internal_gaps_minio(file_dates, gap_threshold))
            
            return gaps
            
        except Exception as e:
            print(f"Error detecting gaps: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _detect_boundary_gaps_minio(self, file_dates, gap_threshold):
        """
        Detect gaps between consecutive files in MinIO.
        
        Compares the maximum timestamp of a file with the minimum timestamp
        of the next file. If the gap exceeds the threshold, it's reported.
        
        Args:
            file_dates: List of (date_str, object_path) tuples, sorted by date
            gap_threshold: Minimum gap duration in milliseconds to report
        
        Returns:
            List of boundary gap dictionaries
        """
        gaps = []
        timestamp_field = self.get_timestamp_field()
        
        for i in range(len(file_dates) - 1):
            try:
                current_date, current_path = file_dates[i]
                next_date, next_path = file_dates[i + 1]
                
                # Read timestamp columns from MinIO
                current_df = self.minio_writer.read_parquet(current_path)
                next_df = self.minio_writer.read_parquet(next_path)
                
                if current_df is None or next_df is None:
                    continue
                
                # Get last timestamp of current file and first timestamp of next file
                current_end = current_df[timestamp_field].max()
                next_start = next_df[timestamp_field].min()
                
                # Check if gap exceeds threshold
                gap_duration = next_start - current_end
                if gap_duration > gap_threshold:
                    gaps.append({
                        'type': 'boundary',
                        'start': current_end + 1,
                        'end': next_start - 1,
                        'duration_hours': gap_duration / (1000 * 3600),
                        'between_files': f"{current_date}.parquet → {next_date}.parquet"
                    })
                    
            except Exception as e:
                print(f"Error checking boundary gap between {file_dates[i][0]} and {file_dates[i+1][0]}: {e}")
                continue
        
        return gaps
    
    def _detect_internal_gaps_minio(self, file_dates, gap_threshold):
        """
        Detect gaps within individual files in MinIO.
        
        Args:
            file_dates: List of (date_str, object_path) tuples
            gap_threshold: Minimum gap duration in milliseconds to report
        
        Returns:
            List of internal gap dictionaries
        """
        gaps = []
        
        for date_str, object_path in file_dates:
            try:
                file_gaps = self._check_single_file_gaps_minio(date_str, object_path, gap_threshold)
                gaps.extend(file_gaps)
            except Exception as e:
                print(f"Error checking internal gaps in {date_str}: {e}")
                continue
        
        return gaps
    
    def _check_single_file_gaps_minio(self, date_str, object_path, gap_threshold):
        """
        Check for gaps within a single file from MinIO.
        
        Sorts timestamps and checks consecutive pairs for gaps.
        Uses sampling for very large files to avoid memory issues.
        
        Args:
            date_str: Date string from filename (e.g., "2025-01-12" or "2025-01")
            object_path: Full path to object in MinIO
            gap_threshold: Minimum gap duration in milliseconds to report
        
        Returns:
            List of internal gap dictionaries for this file
        """
        try:
            timestamp_field = self.get_timestamp_field()
            
            # Read from MinIO
            df = self.minio_writer.read_parquet(object_path)
            
            if df is None:
                return []
            
            # Sample if too large to avoid memory issues
            if len(df) > 10_000_000:
                df = df.sample(n=50_000)
            
            # Sort timestamps and check for gaps
            timestamps = df[timestamp_field].sort().to_list()
            gaps = []
            
            for i in range(len(timestamps) - 1):
                gap_duration = timestamps[i+1] - timestamps[i]
                if gap_duration > gap_threshold:
                    gaps.append({
                        'type': 'internal',
                        'start': timestamps[i] + 1,
                        'end': timestamps[i+1] - 1,
                        'duration_hours': gap_duration / (1000 * 3600),
                        'inside_file': f"{date_str}.parquet"
                    })
            
            return gaps
            
        except Exception as e:
            print(f"Error reading {date_str}: {e}")
            return []
    
    # ==================== Common Gap Display Methods ====================
    
    def show_gaps(self, gaps):
        """
        Display detected gaps in a formatted manner.
        
        Args:
            gaps: List of gap dictionaries from detect_gaps()
        """
        if not gaps:
            print("No gaps found")
            return
        
        # Count different types of gaps
        boundary_count = len([g for g in gaps if g['type'] == 'boundary'])
        internal_count = len([g for g in gaps if g['type'] == 'internal'])
        pre_history_count = len([g for g in gaps if g['type'] == 'pre_history'])
        
        # Display summary
        if pre_history_count > 0:
            print(f"Pre-history gaps: {pre_history_count}")
        if boundary_count > 0:
            print(f"Boundary gaps: {boundary_count}")
        if internal_count > 0:
            print(f"Internal gaps: {internal_count}")
        
        # Display each gap in detail
        for i, gap in enumerate(gaps):
            start_dt = datetime.fromtimestamp(gap['start'] / 1000, tz=timezone.utc)
            end_dt = datetime.fromtimestamp(gap['end'] / 1000, tz=timezone.utc)
            
            print(f"\nGap {i+1}/{len(gaps)} ({gap['type']}):")
            print(f"    Time: {start_dt} → {end_dt}")
            print(f"    Duration: {gap['duration_hours']:.2f} hours")
            
            if gap['type'] == 'boundary':
                print(f"    Between files: {gap['between_files']}")
            elif gap['type'] in ['initial', 'pre_history']:
                print(f"    Description: {gap.get('description', 'N/A')}")
            else:
                print(f"    Inside file: {gap['inside_file']}")
    
    # ==================== Common Gap Filling Methods ====================
    
    async def fill_gaps(self, gaps, **kwargs):
        """
        Fill detected gaps by fetching data from API.
        
        Args:
            gaps: List of gap dictionaries from detect_gaps()
            **kwargs: Additional parameters (e.g., interval='1m' for klines)
        
        Returns:
            int: Total number of records filled
        """
        if not gaps:
            return 0
            
        total_filled = 0
        for i, gap in enumerate(gaps):
            print(f"\nFilling gap {i+1}/{len(gaps)}:")
            
            filled_count = await self._fetch_gap_data(gap['start'], gap['end'], **kwargs)
            if not filled_count:
                print("\tNo data found")
                continue
            
            print(f"    Completed: {filled_count} records filled")
            total_filled += filled_count
        
        return total_filled
    
    async def _fetch_gap_data(self, start_ms, end_ms, **kwargs):
        """
        Fetch data for a specific gap from API.
        
        This method implements the pagination logic to fetch all data in the gap range:
        1. Start from end_ms and paginate backwards
        2. Transform and buffer data
        3. Save in batches
        4. Continue until start_ms is reached
        
        Args:
            start_ms: Start timestamp in milliseconds
            end_ms: End timestamp in milliseconds
            **kwargs: Additional parameters (e.g., interval for klines)
        
        Returns:
            int: Number of records saved
        """
        data_buffer = []
        current_after = str(end_ms)
        total_saved = 0
        retry_count = 0
        
        while True:
            try:
                # Check for cancellation
                if asyncio.current_task().cancelled():
                    break
                
                # Fetch data from API (implemented by subclass)
                batch = await self.fetch_data_from_api(start_ms, end_ms, current_after, **kwargs)
                
                if not batch:
                    break
                
                # Reset retry counter on success
                retry_count = 0
                
                min_ts = None
                
                # Process each item in the batch
                for item in batch:
                    ts = self._extract_timestamp(item)
                    
                    # Only keep records within the gap range
                    if start_ms <= ts <= end_ms:
                        transformed_data = self.transform_api_response(item)
                        data_buffer.append(transformed_data)
                    
                    # Track minimum timestamp for pagination
                    if min_ts is None or ts < min_ts:
                        min_ts = ts
                
                # Save buffer when it reaches threshold
                if len(data_buffer) >= self.buffer_size:
                    saved_count = self._save_gap_data(data_buffer, **kwargs)
                    total_saved += saved_count
                    data_buffer.clear()
                
                # Stop if we've gone past the start of the gap
                if min_ts and min_ts < start_ms:
                    break
                
                # Update pagination cursor
                current_after = str(min_ts)
                
                # Small delay to allow interruption and respect rate limits
                await asyncio.sleep(0.01)
                
            except KeyboardInterrupt:
                print("Fetching interrupted by user")
                break
            except asyncio.CancelledError:
                print("Fetching cancelled")
                break
            except Exception as e:
                retry_count += 1
                wait_time = 10  # Fixed 10s for all errors
                
                print(f"API error (retry {retry_count}): {e}")
                print(f"Waiting {wait_time}s before retry...")
                await asyncio.sleep(wait_time)
                continue  # Retry indefinitely
        
        # Save remaining data in buffer
        if data_buffer:
            saved_count = self._save_gap_data(data_buffer, **kwargs)
            total_saved += saved_count
        
        return total_saved
    
    def _save_gap_data(self, data_records, **kwargs):
        """
        Save gap data grouped by file pattern (daily or monthly).
        
        This method:
        1. Groups records by date (daily) or month (monthly) based on get_file_pattern()
        2. Saves each group to its corresponding file
        
        Args:
            data_records: List of data records (dictionaries)
            **kwargs: Additional parameters (not used currently)
        
        Returns:
            int: Number of records saved
        """
        file_pattern = self.get_file_pattern()
        timestamp_field = self.get_timestamp_field()
        
        # Group by date or month
        data_by_period = {}
        for record in data_records:
            if file_pattern == 'daily':
                # Format: 2025-01-12
                period_str = datetime.fromtimestamp(
                    record[timestamp_field] / 1000, 
                    tz=timezone.utc
                ).strftime('%Y-%m-%d')
            else:  # monthly
                # Format: 2025-01
                period_str = datetime.fromtimestamp(
                    record[timestamp_field] / 1000, 
                    tz=timezone.utc
                ).strftime('%Y-%m')
            
            data_by_period.setdefault(period_str, []).append(record)
        
        # Save each period separately
        total_saved = 0
        for period_str, period_data in data_by_period.items():
            self._save_to_file(period_str, period_data, **kwargs)
            total_saved += len(period_data)
        
        return total_saved
    
    def _transform_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Optional hook for subclasses to transform DataFrame before saving.
        Override this in subclass if you need schema enforcement or other transformations.
        
        Args:
            df: Input DataFrame
        
        Returns:
            Transformed DataFrame (default: returns input unchanged)
        """
        return df
    
    def _save_to_file(self, period_str, data_records, **kwargs):
        """
        Save data records to a parquet file in MinIO.
        
        This method:
        1. Converts records to Polars DataFrame
        2. Applies optional transformation via _transform_dataframe()
        3. Checks if file exists in MinIO
        4. If exists, merges with existing data (deduplicates by unique field)
        5. Writes final DataFrame to MinIO
        
        Args:
            period_str: Period string (e.g., "2025-01-12" for daily, "2025-01" for monthly)
            data_records: List of data records (dictionaries)
            **kwargs: Additional parameters (not used currently)
        """
        timestamp_field = self.get_timestamp_field()
        unique_field = self.get_unique_field()
        
        # Convert to DataFrame and sort by timestamp
        df = pl.DataFrame(data_records).sort(timestamp_field)
        
        # Apply optional transformation (e.g., schema enforcement)
        df = self._transform_dataframe(df)
        
        # MinIO path: {data_type}/{symbol}/{period}.parquet
        object_path = f"{self.data_type}/{self.symbol.lower()}/{period_str}.parquet"
        
        # Check if file already exists in MinIO
        existing_df = self.minio_writer.read_parquet(object_path)
        
        if existing_df is not None:
            # Apply transformation to existing data too
            existing_df = self._transform_dataframe(existing_df)
            
            # Merge with existing data and deduplicate
            combined_df = pl.concat([existing_df, df]).unique(
                subset=[unique_field], 
                keep="last"
            ).sort(timestamp_field)
            final_df = combined_df
        else:
            final_df = df
        
        # Write to MinIO
        if self.minio_writer.write_parquet(final_df, object_path):
            print(f"    {len(data_records)} → s3://{self.minio_writer.bucket}/{object_path}")
        else:
            print(f"    Failed to write {len(data_records)} records to MinIO")
    
    # ==================== Main Run Method ====================
    
    async def run(self):
        """
        Main run method - can be customized by subclasses if needed.
        
        Default behavior:
        1. Detect gaps with 60-second threshold
        2. Display found gaps
        3. Fill gaps if any exist
        """
        gaps = self.detect_gaps(gap_threshold=60000)
        self.show_gaps(gaps)
        
        if gaps:
            await self.fill_gaps(gaps)
