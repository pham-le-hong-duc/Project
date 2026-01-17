import tempfile
import gc
import time
import sys
import io
from pathlib import Path
from datetime import datetime, timedelta, timezone

import orjson

import requests
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

# Add parent directory to path for minio_helper import
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.s3_client import MinIOWriter

class PerpetualOrderBookDownload:
    def __init__(self, symbol="BTC-USDT-SWAP", 
                 base_start_date="2025-01-01",
                 minio_endpoint=None,
                 minio_access_key=None,
                 minio_secret_key=None,
                 minio_bucket="okx",
                 chunk_size=2000000):
        self.symbol = symbol
        self.base_start_date = base_start_date
        self.data_type = "perpetual_orderBook"
        self.chunk_size = chunk_size
        self.daily_url_template = "https://static.okx.com/cdn/okx/match/orderbook/L2/400lv/daily/{YYYYMMDD}/{SYMBOL}-L2orderbook-400lv-{YYYY_MM_DD}.tar.gz"
        
        # Initialize MinIO writer
        self.minio_writer = MinIOWriter(
            endpoint=minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            bucket=minio_bucket,
            secure=False
        )
        print(f"Using MinIO storage: s3://{minio_bucket or 'okx'}/{self.data_type}/{symbol.lower()}/")
        
        self.schema = pa.schema([
            ('instId', pa.string()),
            ('action', pa.string()),
            ('ts', pa.int64()),
            ('asks', pa.string()),
            ('bids', pa.string())
        ])
    
    def get_url(self, date_obj):
        """Generate URL for a specific date"""
        return self.daily_url_template.format(
            YYYYMMDD=date_obj.strftime("%Y%m%d"),
            SYMBOL=self.symbol,
            YYYY_MM_DD=date_obj.strftime("%Y-%m-%d")
        )
    
    def _is_download_available(self, url):
        """Check if download is available using HEAD request"""
        try:
            return requests.head(url, timeout=30).status_code == 200
        except:
            return False
    
    def get_available_files_list(self):
        """Get list of available files from 2025-01-01 to today"""
        today = datetime.now(timezone.utc).date()
        start_date = datetime(2025, 1, 1).date()
        available_files = []
        
        current_date = start_date
        
        while current_date <= today:
            if self._is_download_available(self.get_url(current_date)):
                available_files.append(current_date)
            current_date += timedelta(days=1)
        
        print(f"Found {len(available_files)} available files")
        return available_files
    
    def _is_file_complete_by_rows(self, date_str: str, expected_rows: int = 1440) -> bool:
        """True if parquet exists in MinIO and has exactly expected_rows records."""
        try:
            object_path = f"{self.data_type}/{self.symbol.lower()}/{date_str}.parquet"
            
            # Read from MinIO to check
            df = self.minio_writer.read_parquet(object_path)
            if df is None:
                return False
            
            return len(df) == expected_rows
        except Exception:
            return False

    def download_date_range(self, dates_list):
        """Tải và tái tạo 1440 snapshot/phút cho mỗi ngày. Nếu file hiện có ≠1440, vẫn tải và ghi đè."""
        results = {"success": 0, "failed": 0, "skipped": 0}
        
        for date_obj in dates_list:
            date_str = date_obj.strftime('%Y-%m-%d')
            
            if self._is_file_complete_by_rows(date_str, expected_rows=1440):
                print(f"Skip {date_str}.parquet (đã đủ 1440 snapshot)")
                results["skipped"] += 1
                continue
            
            if self._download_and_reconstruct_sync(self.get_url(date_obj), date_obj, date_str):
                print(f"{date_str}.parquet → MinIO")
                results["success"] += 1
            else:
                results["failed"] += 1
        
        return results["success"]
    
    def _download_and_reconstruct_sync(self, url, date_obj, date_str):
        """Tải tar.gz và tái tạo 1440 snapshot/phút từ snapshot+update, ghi vào MinIO."""
        try:
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()
            return self._reconstruct_sync(response.content, date_obj, date_str)
        except Exception as e:
            print(f"Download error: {e}")
            return False
    
    def _reconstruct_sync(self, content, date_obj, date_str):
        """Tái tạo 1440 snapshot/phút từ nội dung tar.gz (đọc cả update). Ghi vào MinIO."""
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.tar.gz') as tmp_file:
                tmp_file.write(content)
                tmp_path = tmp_file.name
            import tarfile
            try:
                with tarfile.open(tmp_path, 'r:gz') as tar_file:
                    files = tar_file.getnames()
                    data_files = [f for f in files if f.endswith('.data')]
                    csv_files = [f for f in files if f.endswith('.csv')]
                    if data_files:
                        return self._reconstruct_from_data_file(tar_file, data_files[0], date_obj, date_str)
                    elif csv_files:
                        return self._reconstruct_from_csv_file(tar_file, csv_files[0], date_obj, date_str)
                    else:
                        return False
            finally:
                Path(tmp_path).unlink(missing_ok=True)
        except Exception as e:
            print(f"Reconstruct error: {e}")
            return False
    
    # Legacy converters (giữ lại nếu cần dùng đường nhanh chỉ-snapshot)
    def _process_data_file(self, tar_file, data_file, tmp_path, output_file):
        """Process .data file"""
        tar_file.extract(data_file, Path(tmp_path).parent)
        data_path = Path(tmp_path).parent / data_file
        
        try:
            return self._convert_l400_to_l50_optimized(data_path, output_file)
        finally:
            data_path.unlink(missing_ok=True)
    
    def _process_csv_file(self, tar_file, csv_file, tmp_path, output_file):
        """Process .csv file"""
        tar_file.extract(csv_file, Path(tmp_path).parent)
        csv_path = Path(tmp_path).parent / csv_file
        
        try:
            return self._convert_csv_optimized_from_file(csv_path, output_file)
        finally:
            csv_path.unlink(missing_ok=True)
    
    def _convert_l400_to_l50_optimized(self, data_path, output_file):
        """Convert .data file (JSON lines) to L50 parquet"""
        total_written = 0
        
        try:
            with pq.ParquetWriter(output_file, self.schema, compression='snappy') as writer:
                with open(data_path, 'r') as f:
                    lines_batch = []
                    
                    for line in f:
                        lines_batch.append(line.strip())
                        
                        if len(lines_batch) >= self.chunk_size:
                            total_written += self._process_json_lines_batch(lines_batch, writer)
                            lines_batch = []
                    
                    if lines_batch:
                        total_written += self._process_json_lines_batch(lines_batch, writer)
            
        except:
            return 0
            
        return total_written
    
    def _process_json_lines_batch(self, lines_batch, writer):
        """Process batch of JSON lines"""
        data = {'inst_ids': [], 'actions': [], 'timestamps': [], 'asks': [], 'bids': []}
        
        for line in lines_batch:
            if not line:
                continue
                
            try:
                json_data = orjson.loads(line.encode())
                
                # Filter: only process rows where action == 'snapshot'
                action_val = str(json_data.get('action', '')).lower()
                if action_val != 'snapshot':
                    continue
                
                asks = json_data.get('asks', [])
                bids = json_data.get('bids', [])
                
                if asks and bids:
                    data['inst_ids'].append(json_data.get('instId', ''))
                    data['actions'].append(json_data.get('action', ''))
                    data['timestamps'].append(int(json_data.get('ts', 0)))
                    
                    # Only after filtering do we cut to L50 and serialize
                    asks_l50 = orjson.dumps(asks[:50]).decode()
                    bids_l50 = orjson.dumps(bids[:50]).decode()
                    
                    data['asks'].append(asks_l50)
                    data['bids'].append(bids_l50)
                    
            except:
                continue
        
        if data['inst_ids']:
            table = pa.Table.from_arrays([
                pa.array(data['inst_ids']),
                pa.array(data['actions']),
                pa.array(data['timestamps']),
                pa.array(data['asks']),
                pa.array(data['bids'])
            ], schema=self.schema)
            
            writer.write_table(table)
        
        return len(data['inst_ids'])
    
    def _convert_csv_optimized_from_file(self, csv_file_path, output_file):
        """Convert CSV file to parquet"""
        total_written = 0
        
        with pq.ParquetWriter(output_file, self.schema, compression='snappy') as writer:
            with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
                lines_batch = []
                
                for line in csv_file:
                    line = line.strip()
                    if not line or line.startswith('ts'):
                        continue
                    
                    lines_batch.append(line)
                    
                    if len(lines_batch) >= self.chunk_size:
                        total_written += self._process_csv_batch(lines_batch, writer)
                        lines_batch = []
                
                if lines_batch:
                    total_written += self._process_csv_batch(lines_batch, writer)
        
        return total_written
    
    def _process_csv_batch(self, lines_batch, writer):
        """Process batch of CSV lines.
        Expected minimal format per line: ts,bids_json,asks_json
        Optional 4th column: action. If present, we only keep rows where action == 'snapshot'.
        If absent, we default to 'snapshot'.
        """
        data = {'inst_ids': [], 'actions': [], 'timestamps': [], 'asks': [], 'bids': []}
        
        for line in lines_batch:
            parts = line.split(',')
            if len(parts) >= 3:
                try:
                    ts = int(parts[0])
                    # Determine action: use 4th column if available, else default to 'snapshot'
                    action_val = parts[3].strip() if len(parts) >= 4 else "snapshot"
                    # Filter: keep only action == 'snapshot'
                    if action_val.lower() != "snapshot":
                        continue

                    bids_data = orjson.loads(parts[1].encode())
                    asks_data = orjson.loads(parts[2].encode())
                    
                    if bids_data and asks_data:
                        data['inst_ids'].append(self.symbol)
                        data['actions'].append("snapshot")
                        data['timestamps'].append(ts)
                        
                        # Convert to L50 and serialize
                        bids_l50 = orjson.dumps(bids_data[:50]).decode()
                        asks_l50 = orjson.dumps(asks_data[:50]).decode()
                        
                        data['asks'].append(asks_l50)
                        data['bids'].append(bids_l50)
                        
                except:
                    continue
        
        if data['inst_ids']:
            table = pa.Table.from_arrays([
                pa.array(data['inst_ids']),
                pa.array(data['actions']),
                pa.array(data['timestamps']),
                pa.array(data['asks']),
                pa.array(data['bids'])
            ], schema=self.schema)
            
            writer.write_table(table)
        
        return len(data['inst_ids'])
    
    # ====== Reconstruction helpers ======
    def _emit_minute_snapshots(self, state, date_obj, writer):
        """Ghi ra đúng 1440 snapshot/phút từ state.scheduled_emits (danh sách (minute_ts, asks, bids))."""
        total = 0
        for minute_ts, asks, bids in state.scheduled_emits:
            table = pa.Table.from_arrays([
                pa.array([self.symbol]),
                pa.array(["snapshot"]),
                pa.array([int(minute_ts)]),
                pa.array([orjson.dumps(asks[:50]).decode()]),
                pa.array([orjson.dumps(bids[:50]).decode()]),
            ], schema=self.schema)
            writer.write_table(table)
            total += 1
        return total

    class _BookState:
        def __init__(self):
            self.bids = {}  # price -> size
            self.asks = {}
            self.have_state = False
            self.scheduled_emits = []  # list of (minute_ts, asks_list, bids_list)

        @staticmethod
        def _level_list_from_maps(bids_map, asks_map):
            # best bids: desc by price; best asks: asc by price
            bids = [[p, s] for p, s in bids_map.items() if s and s > 0]
            asks = [[p, s] for p, s in asks_map.items() if s and s > 0]
            bids.sort(key=lambda x: x[0], reverse=True)
            asks.sort(key=lambda x: x[0])
            # truncate to 50 in emit function
            return bids, asks

        def apply_snapshot(self, bids_levels, asks_levels):
            # Levels may be [price, size] or [price, size, ...]; values often are strings
            new_bids = {}
            new_asks = {}
            try:
                for lvl in bids_levels or []:
                    if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
                        continue
                    p, s = lvl[0], lvl[1]
                    try:
                        p = float(p); s = float(s)
                    except Exception:
                        continue
                    if s > 0:
                        new_bids[p] = s
                for lvl in asks_levels or []:
                    if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
                        continue
                    p, s = lvl[0], lvl[1]
                    try:
                        p = float(p); s = float(s)
                    except Exception:
                        continue
                    if s > 0:
                        new_asks[p] = s
            except Exception:
                pass
            self.bids = new_bids
            self.asks = new_asks
            self.have_state = True

        def apply_update(self, bids_levels, asks_levels):
            # OKX update: each level like [price, size, ...]; size 0 removes the level
            try:
                for lvl in bids_levels or []:
                    if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
                        continue
                    p, s = lvl[0], lvl[1]
                    try:
                        p = float(p); s = float(s)
                    except Exception:
                        continue
                    if s <= 0:
                        self.bids.pop(p, None)
                    else:
                        self.bids[p] = s
                for lvl in asks_levels or []:
                    if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
                        continue
                    p, s = lvl[0], lvl[1]
                    try:
                        p = float(p); s = float(s)
                    except Exception:
                        continue
                    if s <= 0:
                        self.asks.pop(p, None)
                    else:
                        self.asks[p] = s
            except Exception:
                pass

        def emit_at(self, minute_ts):
            bids, asks = self._level_list_from_maps(self.bids, self.asks)
            self.scheduled_emits.append((minute_ts, asks, bids))

    def _reconstruct_from_data_file(self, tar_file, data_file, date_obj, date_str):
        import tarfile
        # Extract to temp path
        tmp_dir = Path(tempfile.gettempdir())
        tar_file.extract(data_file, tmp_dir)
        data_path = tmp_dir / data_file
        try:
            day_start = int(datetime(date_obj.year, date_obj.month, date_obj.day, tzinfo=timezone.utc).timestamp() * 1000)
            minutes = [day_start + i * 60000 for i in range(1440)]
            state = self._BookState()
            next_emit_index = 0
            current_minute_ts = minutes[next_emit_index]

            # First pass: parse lines and roll state, emitting when crossing minute boundaries
            with open(data_path, 'r') as f:
                for line in f:
                    s = line.strip()
                    if not s:
                        continue
                    try:
                        obj = orjson.loads(s.encode())
                    except Exception:
                        continue
                    ts = int(obj.get('ts', 0))
                    # allow slight jitter: treat minute boundary as floor(ts/60000)*60000
                    minute_ts = (ts // 60000) * 60000
                    # Fill any missing earlier minutes if we have state
                    while next_emit_index < 1440 and minutes[next_emit_index] <= minute_ts:
                        if state.have_state:
                            state.emit_at(minutes[next_emit_index])
                        next_emit_index += 1
                    action = str(obj.get('action', '')).lower()
                    bids = obj.get('bids', []) or []
                    asks = obj.get('asks', []) or []
                    if action == 'snapshot':
                        state.apply_snapshot(bids, asks)
                    else:
                        # treat as update
                        state.apply_update(bids, asks)
            # After stream ends, emit remaining minutes using last state
            while next_emit_index < 1440:
                if state.have_state:
                    state.emit_at(minutes[next_emit_index])
                next_emit_index += 1

            # If we never had state, cannot reconstruct; return False
            if not state.scheduled_emits:
                return False

            # If first state appears late: forward-fill earlier minutes with first known state
            if len(state.scheduled_emits) < 1440:
                first_minute_ts, first_asks, first_bids = state.scheduled_emits[0]
                missing = []
                first_index = minutes.index(first_minute_ts) if first_minute_ts in minutes else 0
                for i in range(0, first_index):
                    missing.append((minutes[i], first_asks, first_bids))
                state.scheduled_emits = missing + state.scheduled_emits
            # Truncate/exact to 1440
            state.scheduled_emits = state.scheduled_emits[:1440]

            # Write parquet to buffer then upload to MinIO
            buffer = io.BytesIO()
            with pq.ParquetWriter(buffer, self.schema, compression='snappy') as writer:
                written = self._emit_minute_snapshots(state, date_obj, writer)
            
            if written == 1440:
                buffer.seek(0)
                object_path = f"{self.data_type}/{self.symbol.lower()}/{date_str}.parquet"
                # Upload to MinIO
                try:
                    self.minio_writer.client.put_object(
                        self.minio_writer.bucket,
                        object_path,
                        buffer,
                        length=buffer.getbuffer().nbytes,
                        content_type='application/octet-stream'
                    )
                    return True
                except Exception as e:
                    print(f"MinIO upload error: {e}")
                    return False
            return False
        except Exception as e:
            print(f"Reconstruct .data error: {e}")
            return False
        finally:
            try:
                data_path.unlink(missing_ok=True)
            except Exception:
                pass

    def _reconstruct_from_csv_file(self, tar_file, csv_file, date_obj, date_str):
        tmp_dir = Path(tempfile.gettempdir())
        tar_file.extract(csv_file, tmp_dir)
        csv_path = tmp_dir / csv_file
        try:
            day_start = int(datetime(date_obj.year, date_obj.month, date_obj.day, tzinfo=timezone.utc).timestamp() * 1000)
            minutes = [day_start + i * 60000 for i in range(1440)]
            state = self._BookState()
            next_emit_index = 0

            with open(csv_path, 'r', encoding='utf-8') as f:
                for raw in f:
                    line = raw.strip()
                    if not line or line.startswith('ts'):
                        continue
                    parts = line.split(',')
                    if len(parts) < 3:
                        continue
                    try:
                        ts = int(parts[0])
                        bids = orjson.loads(parts[1].encode())
                        asks = orjson.loads(parts[2].encode())
                        action_val = parts[3].strip().lower() if len(parts) >= 4 else 'update'
                    except Exception:
                        continue
                    minute_ts = (ts // 60000) * 60000
                    while next_emit_index < 1440 and minutes[next_emit_index] <= minute_ts:
                        if state.have_state:
                            state.emit_at(minutes[next_emit_index])
                        next_emit_index += 1
                    if action_val == 'snapshot':
                        state.apply_snapshot(bids, asks)
                    else:
                        state.apply_update(bids, asks)
            while next_emit_index < 1440:
                if state.have_state:
                    state.emit_at(minutes[next_emit_index])
                next_emit_index += 1
            if not state.scheduled_emits:
                return False
            if len(state.scheduled_emits) < 1440:
                first_minute_ts, first_asks, first_bids = state.scheduled_emits[0]
                missing = []
                first_index = minutes.index(first_minute_ts) if first_minute_ts in minutes else 0
                for i in range(0, first_index):
                    missing.append((minutes[i], first_asks, first_bids))
                state.scheduled_emits = missing + state.scheduled_emits
            state.scheduled_emits = state.scheduled_emits[:1440]
            
            # Write parquet to buffer then upload to MinIO
            buffer = io.BytesIO()
            with pq.ParquetWriter(buffer, self.schema, compression='snappy') as writer:
                written = self._emit_minute_snapshots(state, date_obj, writer)
            
            if written == 1440:
                buffer.seek(0)
                object_path = f"{self.data_type}/{self.symbol.lower()}/{date_str}.parquet"
                # Upload to MinIO
                try:
                    self.minio_writer.client.put_object(
                        self.minio_writer.bucket,
                        object_path,
                        buffer,
                        length=buffer.getbuffer().nbytes,
                        content_type='application/octet-stream'
                    )
                    return True
                except Exception as e:
                    print(f"MinIO upload error: {e}")
                    return False
            return False
        except Exception as e:
            print(f"Reconstruct .csv error: {e}")
            return False
        finally:
            try:
                csv_path.unlink(missing_ok=True)
            except Exception:
                pass

    def run_bootstrap(self):
        """Phase 1: Bootstrap initial data"""
        print("Phase 1: Bootstrapping initial data...")
        
        try:
            available_dates = self.get_available_files_list()
            
            # Determine files to download strictly by row-count completeness (1440 rows)
            files_to_download = []
            for date in available_dates:
                date_str = date.strftime('%Y-%m-%d')
                if not self._is_file_complete_by_rows(date_str, expected_rows=1440):
                    files_to_download.append(date)
                else:
                    print(f"Skip {date_str}.parquet (đã đủ 1440 snapshot)")

            if files_to_download:
                self.download_date_range(files_to_download)
            
            return set(available_dates)
            
        except Exception as e:
            print(f"Bootstrap error: {e}")
            return set()
    
    def run(self):
        """Main run method: Bootstrap only (for scheduled runs)"""
        print("Running OrderBook bootstrap...")
        self.run_bootstrap()
        print("✅ Bootstrap completed!")

if __name__ == "__main__":
    downloader = PerpetualOrderBookDownload()
    downloader.run()