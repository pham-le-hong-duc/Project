"""
Real-time Aggregation Consumer for Silver Layer.

Strategy:
1. Load 2 recent S3 files → Unified block in RAM
2. Poll RedPanda → Append to block
3. Every 5m boundary (UTC) → Aggregate all intervals (5m, 15m, 1h, 4h, 1d)
4. UPSERT to TimescaleDB
5. When date changes → Drop oldest day
"""
import json
import signal
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
from kafka import KafkaConsumer
import polars as pl
import time

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from utils.timescaledb_client import TimescaleDBClient
from utils.s3_client import MinIOWriter


class RealtimeAggregator:
    """Base consumer for real-time aggregation (Silver layer)."""
    
    def __init__(self, topic, data_type, symbol, timestamp_field,
                 intervals=['5m', '15m', '1h', '4h', '1d'],
                 bootstrap_servers='redpanda:9092',
                 group_id=None,
                 db_host=None,
                 s3_days_to_load=2,
                 dedupe_columns=None,
                 warmup_messages=1000):
        
        self.topic = topic
        self.data_type = data_type
        self.symbol = symbol.lower()
        self.timestamp_field = timestamp_field
        self.intervals = intervals
        self.s3_days_to_load = s3_days_to_load
        self.dedupe_columns = dedupe_columns  # Columns to use for deduplication
        self.warmup_messages = warmup_messages  # Number of messages to load on first startup
        
        # Initialize clients
        self.db_client = TimescaleDBClient()
        self.s3_client = MinIOWriter()
        print(f"✅ TimescaleDB connected")
        print(f"✅ S3 client initialized")
        
        # Load historical data from S3
        print(f"📥 Loading {s3_days_to_load} recent days from S3...")
        self.df_historical = self._load_historical_from_s3()
        print(f"✅ Loaded {len(self.df_historical):,} records into RAM")
        
        # Tracking state
        self.current_date = self._get_date_utc(self.df_historical[self.timestamp_field].max())
        self.next_boundary = self._get_next_5m_boundary(int(time.time() * 1000))
        
        print(f"📅 Current date: {self.current_date}")
        print(f"⏰ Next boundary: {self._format_ts(self.next_boundary)}")
        
        # Initialize Kafka consumer
        consumer_group_id = group_id or f"silver-{data_type}-{symbol}"
        
        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=consumer_group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=False,
            max_poll_records=1000,
            session_timeout_ms=30000,
            consumer_timeout_ms=-1
        )
        
        self.consumer.subscribe([topic])
        print(f"✅ Subscribed to topic: {topic}")
        
        # Wait for partition assignment
        partitions = []
        for _ in range(30):
            partitions = self.consumer.assignment()
            if partitions:
                break
            self.consumer.poll(timeout_ms=1000)
        
        if not partitions:
            raise Exception("Failed to get partition assignment")
        
        print(f"✅ Assigned partitions: {partitions}")
        
        # Always warmup on container startup - seek to last N messages per partition
        print(f"🔄 Container startup - loading last {self.warmup_messages} messages for warmup")
        for partition in partitions:
            # Always seek to end to get latest messages
            self.consumer.seek_to_end(partition)
            end_offset = self.consumer.position(partition)
            
            # Seek back N messages (or to beginning if < N)
            target_offset = max(0, end_offset - self.warmup_messages)
            self.consumer.seek(partition, target_offset)
            
            print(f"   Partition {partition.partition}: end={end_offset}, loading from offset={target_offset}")
        
        # Poll and load these N messages into RAM immediately
        print(f"📥 Loading last {self.warmup_messages} messages into RAM...")
        warmup_records = []
        while True:
            messages = self.consumer.poll(timeout_ms=5000, max_records=self.warmup_messages)
            if not messages:
                break
            
            for topic_partition, msgs in messages.items():
                for msg in msgs:
                    warmup_records.append(msg.value)
            
            # Stop if we've collected enough or reached current position
            if len(warmup_records) >= self.warmup_messages * len(partitions):
                break
        
        if warmup_records:
            df_warmup = pl.DataFrame(warmup_records)
            self.df_historical = pl.concat([self.df_historical, df_warmup], how="vertical_relaxed")
            
            # Dedupe after loading warmup data
            before_dedupe = len(self.df_historical)
            if self.dedupe_columns:
                self.df_historical = self.df_historical.unique(
                    subset=self.dedupe_columns,
                    keep='last',
                    maintain_order=False
                )
            after_dedupe = len(self.df_historical)
            
            print(f"✅ Loaded {len(warmup_records):,} warmup records (RAM now: {after_dedupe:,}, Deduped: {before_dedupe - after_dedupe})")
        else:
            print(f"⚠️  No warmup records available")
        
        self.running = True
        self.total_consumed = 0
        self.total_aggregated = 0
    
    def _load_historical_from_s3(self):
        """Load N recent days from S3 into unified DataFrame."""
        s3_prefix = f"{self.data_type}/{self.symbol}"
        all_files = sorted(self.s3_client.list_objects(prefix=s3_prefix, recursive=True))
        parquet_files = [f for f in all_files if f.endswith('.parquet')]
        
        if not parquet_files:
            print("⚠️  No S3 files found, starting with empty DataFrame")
            return pl.DataFrame()
        
        # Get N most recent files
        recent_files = parquet_files[-self.s3_days_to_load:]
        
        dfs = []
        for file_path in recent_files:
            print(f"   Loading: {file_path}")
            df = self.s3_client.read_parquet(file_path)
            if df is not None and len(df) > 0:
                dfs.append(df)
        
        if not dfs:
            return pl.DataFrame()
        
        # Concat all
        df_combined = pl.concat(dfs, how="vertical_relaxed")
        
        # Sort by timestamp
        df_sorted = df_combined.sort(self.timestamp_field)
        
        return df_sorted
    
    def _get_date_utc(self, ts_ms):
        """Get date from timestamp (UTC)."""
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        return dt.date()
    
    def _get_next_5m_boundary(self, current_ts_ms):
        """Get next 5m boundary timestamp (UTC)."""
        dt = datetime.fromtimestamp(current_ts_ms / 1000, tz=timezone.utc)
        
        # Round up to next 5m
        minutes = ((dt.minute // 5) + 1) * 5
        
        if minutes >= 60:
            boundary_dt = dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        else:
            boundary_dt = dt.replace(minute=minutes, second=0, microsecond=0)
        
        return int(boundary_dt.timestamp() * 1000)
    
    def _get_window_ts(self, ts_ms, interval):
        """Get window start timestamp for given timestamp and interval."""
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        
        if interval == '5m':
            minutes = (dt.minute // 5) * 5
            window_dt = dt.replace(minute=minutes, second=0, microsecond=0)
        elif interval == '15m':
            minutes = (dt.minute // 15) * 15
            window_dt = dt.replace(minute=minutes, second=0, microsecond=0)
        elif interval == '1h':
            window_dt = dt.replace(minute=0, second=0, microsecond=0)
        elif interval == '4h':
            hours = (dt.hour // 4) * 4
            window_dt = dt.replace(hour=hours, minute=0, second=0, microsecond=0)
        elif interval == '1d':
            window_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            raise ValueError(f"Unsupported interval: {interval}")
        
        return int(window_dt.timestamp() * 1000)
    
    def _get_window_size_ms(self, interval):
        """Get window size in milliseconds."""
        sizes = {
            '5m': 5 * 60 * 1000,
            '15m': 15 * 60 * 1000,
            '1h': 60 * 60 * 1000,
            '4h': 4 * 60 * 60 * 1000,
            '1d': 24 * 60 * 60 * 1000
        }
        return sizes[interval]
    
    def _format_ts(self, ts_ms):
        """Format timestamp for display."""
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    
    def aggregate_window(self, df_window, window_ts, interval):
        """
        Aggregate window data to features.
        MUST BE IMPLEMENTED BY SUBCLASS.
        
        Args:
            df_window: Polars DataFrame with records in window
            window_ts: Window timestamp (ms)
            interval: Interval string (e.g., '5m')
        
        Returns:
            pl.DataFrame with aggregated features
        """
        raise NotImplementedError("Subclass must implement aggregate_window()")
    
    def consume(self):
        """
        Main consumption loop implementing the complete mechanism:
        
        PHASE 1: STARTUP (already done in __init__)
        - Load 2 files from S3
        - Initialize tracking (next_boundary, current_date)
        
        PHASE 2: CONTINUOUS LOOP
        1. Poll RedPanda for new messages
        2. Append to unified block (df_historical)
        3. Check if boundary reached (5m UTC)
        4. Aggregate ALL 5 intervals (5m, 15m, 1h, 4h, 1d)
        5. UPSERT to TimescaleDB
        6. Update next boundary
        7. Check date change & drop old data
        """
        print("\n" + "="*60)
        print("STARTING REAL-TIME AGGREGATION CONSUMER")
        print("="*60)
        print(f"Topic: {self.topic}")
        print(f"Data Type: {self.data_type}")
        print(f"Symbol: {self.symbol}")
        print(f"Intervals: {self.intervals}")
        print(f"Current RAM: {len(self.df_historical):,} records")
        print(f"Current Date: {self.current_date}")
        print(f"Next Boundary: {self._format_ts(self.next_boundary)}")
        print("="*60 + "\n")
        
        # Setup signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            print("\n⚠️  Shutdown signal received")
            self.running = False
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            while self.running:
                # ============================================================
                # STEP 1: Poll RedPanda
                # ============================================================
                messages = self.consumer.poll(timeout_ms=1000)
                
                if not messages:
                    # No messages, continue waiting
                    continue
                
                # Extract records from messages
                new_records = []
                for topic_partition, msgs in messages.items():
                    for msg in msgs:
                        new_records.append(msg.value)
                
                if not new_records:
                    continue
                
                self.total_consumed += len(new_records)
                
                # ============================================================
                # STEP 2: Append to unified block
                # ============================================================
                df_new = pl.DataFrame(new_records)
                
                if len(self.df_historical) == 0:
                    self.df_historical = df_new
                else:
                    self.df_historical = pl.concat([self.df_historical, df_new], how="vertical_relaxed")
                
                # Deduplicate (keep last occurrence)
                # This handles potential duplicates from Kafka reprocessing
                before_dedupe = len(self.df_historical)
                
                if self.dedupe_columns:
                    # Dedupe by specified columns (e.g., trade_id for trades)
                    self.df_historical = self.df_historical.unique(
                        subset=self.dedupe_columns,
                        keep='last',
                        maintain_order=False
                    )
                    after_dedupe = len(self.df_historical)
                else:
                    # No deduplication (e.g., for orderbook snapshots, klines)
                    after_dedupe = before_dedupe
                
                duplicates_removed = before_dedupe - after_dedupe
                if duplicates_removed > 0:
                    print(f"📨 Polled {len(new_records)} records (Total: {self.total_consumed:,}, RAM: {after_dedupe:,}, Deduped: {duplicates_removed})")
                else:
                    print(f"📨 Polled {len(new_records)} records (Total: {self.total_consumed:,}, RAM: {after_dedupe:,})")
                
                # ============================================================
                # STEP 3: Check boundary (5m UTC)
                # ============================================================
                max_ts = self.df_historical[self.timestamp_field].max()
                
                if max_ts < self.next_boundary:
                    # Not reached boundary yet
                    print(f"⏳ Waiting for {self._format_ts(self.next_boundary)} (current: {self._format_ts(max_ts)})")
                    continue
                
                # ✅ BOUNDARY REACHED!
                print(f"\n{'='*60}")
                print(f"🎯 BOUNDARY REACHED: {self._format_ts(self.next_boundary)}")
                print(f"{'='*60}")
                
                # ============================================================
                # STEP 4: Aggregate ALL 5 intervals
                # ============================================================
                for interval in self.intervals:
                    try:
                        # 4.1: Calculate window as [X-interval, X) where X = boundary
                        # At boundary 12:30:00, we aggregate:
                        #   - 5m:  [12:25:00, 12:30:00)
                        #   - 15m: [12:15:00, 12:30:00)
                        #   - 1h:  [11:30:00, 12:30:00)
                        #   - 4h:  [08:30:00, 12:30:00)
                        #   - 1d:  [12:30:00 yesterday, 12:30:00 today)
                        
                        window_size_ms = self._get_window_size_ms(interval)
                        window_end = self.next_boundary
                        window_start = window_end - window_size_ms
                        window_ts = window_end  # Use window_end as the timestamp
                        
                        # 4.2: Filter data for this window
                        df_window = self.df_historical.filter(
                            (pl.col(self.timestamp_field) >= window_start) &
                            (pl.col(self.timestamp_field) < window_end)
                        )
                        
                        if len(df_window) == 0:
                            print(f"  ⊘ {interval:>3s} @ {self._format_ts(window_ts)} - No data")
                            continue
                        
                        # 4.3: Aggregate using subclass method
                        aggregated = self.aggregate_window(df_window, window_ts, interval)
                        
                        if aggregated is None or len(aggregated) == 0:
                            print(f"  ⚠️  {interval:>3s} @ {self._format_ts(window_ts)} - Aggregation failed")
                            continue
                        
                        # 4.4: UPSERT to TimescaleDB
                        table_name = f"{self.data_type}_{interval}".lower()
                        
                        # Ensure table exists
                        if not self.db_client.table_exists(table_name):
                            self.db_client.create_table_from_dataframe(aggregated, table_name, replace=False)
                            print(f"  ✅ {interval:>3s} @ {self._format_ts(window_ts)} - Created table & inserted {len(df_window):,} records")
                        else:
                            self.db_client.upsert_dataframe(aggregated, table_name, key_column="ts_ms")
                            print(f"  ✅ {interval:>3s} @ {self._format_ts(window_ts)} - Aggregated {len(df_window):,} records → Upserted 1 row")
                        
                        self.total_aggregated += 1
                        
                    except Exception as e:
                        print(f"  ❌ {interval:>3s} @ {self._format_ts(window_ts)} - Error: {e}")
                        import traceback
                        traceback.print_exc()
                
                # ============================================================
                # STEP 5: Update next boundary
                # ============================================================
                self.next_boundary = self._get_next_5m_boundary(max_ts)
                print(f"\n⏰ Next boundary: {self._format_ts(self.next_boundary)}")
                
                # ============================================================
                # STEP 6: Check date change & drop old data
                # ============================================================
                max_date = self._get_date_utc(max_ts)
                
                if max_date > self.current_date:
                    print(f"\n📅 DATE CHANGED: {self.current_date} → {max_date}")
                    
                    # Drop oldest day (keep only 2 most recent days)
                    cutoff_date = max_date - timedelta(days=1)
                    cutoff_ts = int(datetime.combine(cutoff_date, datetime.min.time()).replace(tzinfo=timezone.utc).timestamp() * 1000)
                    
                    before_count = len(self.df_historical)
                    self.df_historical = self.df_historical.filter(
                        pl.col(self.timestamp_field) >= cutoff_ts
                    )
                    after_count = len(self.df_historical)
                    
                    print(f"🗑️  Dropped {before_count - after_count:,} old records")
                    print(f"💾 RAM now: {after_count:,} records")
                    
                    self.current_date = max_date
                
                # Commit Kafka offsets
                self.consumer.commit()
                print(f"{'='*60}\n")
        
        except Exception as e:
            print(f"\n❌ FATAL ERROR: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            print("\n" + "="*60)
            print("CONSUMER SHUTDOWN")
            print("="*60)
            print(f"Total Consumed: {self.total_consumed:,} records")
            print(f"Total Aggregated: {self.total_aggregated:,} windows")
            print("="*60)
            
            # Close connections
            self.consumer.close()
            print("✅ Kafka consumer closed")
            
            print("✅ TimescaleDB connection remains open (managed externally)")
            print("Shutdown complete.\n")