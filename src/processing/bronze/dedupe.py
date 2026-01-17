#!/usr/bin/env python3
"""
Check and remove duplicate records in MinIO parquet files.
Supports all data types with different schemas.
"""

import polars as pl
from datetime import datetime
import traceback
from src.utils.s3_client import MinIOWriter

class DuplicateChecker:
    def __init__(self, 
                 minio_endpoint=None,
                 minio_access_key=None,
                 minio_secret_key=None,
                 minio_bucket="okx"):
        """
        Initialize DuplicateChecker with MinIO connection.
        
        Args:
            minio_endpoint: MinIO endpoint
            minio_access_key: MinIO access key
            minio_secret_key: MinIO secret key
            minio_bucket: MinIO bucket name
        """
        self.minio = MinIOWriter(
            endpoint=minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            bucket=minio_bucket
        )
        
        # Data configurations with their unique key columns
        self.data_configs = {
            'perpetual_orderBook': ['ts'],
            'perpetual_fundingRate': ['funding_time'],
            'perpetual_trades': ['trade_id'],
            'spot_trades': ['trade_id'],
            'indexPriceKlines': ['open_time'],
            'perpetual_markPriceKlines': ['open_time']
        }
    
    def find_data_paths(self, data_type):
        """Find all symbol paths for a given data type in MinIO."""
        prefix = f"{data_type}/"
        objects = self.minio.list_objects(prefix=prefix, recursive=False)
        
        # Extract unique symbol paths (e.g., 'spot_trades/btc-usdt/')
        paths = set()
        for obj in objects:
            parts = obj.split('/')
            if len(parts) >= 2:
                symbol_path = f"{parts[0]}/{parts[1]}"
                paths.add(symbol_path)
        
        return sorted(list(paths))
    
    def check_file_duplicates(self, object_path, data_type):
        """Check duplicates in a single parquet file from MinIO."""
        try:
            unique_columns = self.data_configs[data_type]
            df = self.minio.read_parquet(object_path)
            
            if df is None:
                return {'status': 'error', 'file': object_path, 'message': 'File not found'}
            
            if df.is_empty():
                return {'status': 'empty', 'file': object_path, 'total_records': 0, 'duplicate_records': 0}
            
            total_records = len(df)
            missing_cols = [col for col in unique_columns if col not in df.columns]
            if missing_cols:
                return {'status': 'error', 'file': object_path, 'message': f'Missing columns: {missing_cols}', 'total_records': total_records}
            
            # Find duplicates using group_by for consistency
            grouped = df.group_by(unique_columns).len()
            duplicated_groups = grouped.filter(pl.col('len') > 1)
            
            duplicate_count = 0
            if len(duplicated_groups) > 0:
                duplicate_count = int(duplicated_groups['len'].sum() - len(duplicated_groups))
            
            return {
                'status': 'checked',
                'file': object_path,
                'total_records': total_records,
                'duplicate_records': duplicate_count
            }
            
        except Exception as e:
            return {'status': 'error', 'file': object_path, 'message': str(e), 'traceback': traceback.format_exc()}
    
    def process_files_for_data_type(self, data_type, action):
        """Process files for a specific data type in MinIO."""
        data_paths = self.find_data_paths(data_type)
        if not data_paths:
            print(f"ERROR: No valid paths found for {data_type}")
            return []
        
        all_results = []
        for symbol_path in data_paths:
            print(f"\nProcessing: {symbol_path}")
            
            # List all parquet files in this symbol path
            parquet_files = [obj for obj in self.minio.list_objects(prefix=f"{symbol_path}/", recursive=True)
                           if obj.endswith('.parquet')]
            parquet_files.sort()
            
            if not parquet_files:
                print("  No parquet files found")
                continue
            
            print(f"  Found {len(parquet_files)} files")
            files_processed = 0
            
            for i, object_path in enumerate(parquet_files, 1):
                filename = object_path.split('/')[-1]
                
                if i % 50 == 0 or i == len(parquet_files):
                    print(f"  Progress: {i}/{len(parquet_files)} ({i*100//len(parquet_files)}%) - {filename}")
                elif i % 10 == 0:
                    print(f"  [{i}/{len(parquet_files)}] {filename}")
                
                if action == 'check':
                    result = self.check_file_duplicates(object_path, data_type)
                    if result.get('duplicate_records', 0) > 0:
                        files_processed += 1
                        filename = object_path.split('/')[-1]
                        print(f"  WARNING: {filename}: {result['duplicate_records']:,} duplicates in {result['total_records']:,} records")
                else:
                    result = self.remove_duplicates_from_file(object_path, data_type)
                    if result.get('had_duplicates', False):
                        files_processed += 1
                
                all_results.append(result)
            
            summary_text = "files have duplicates" if action == 'check' else "files had duplicates"
            print(f"  Summary: {files_processed}/{len(parquet_files)} {summary_text}")
        
        return all_results
    
    def remove_duplicates_from_file(self, object_path, data_type):
        """Remove duplicates from a single parquet file in MinIO, keeping only one record."""
        try:
            unique_columns = self.data_configs[data_type]
            df = self.minio.read_parquet(object_path)
            
            if df is None:
                return {'status': 'error', 'file': object_path, 'message': 'File not found'}
            
            if df.is_empty():
                return {'status': 'empty', 'file': object_path, 'original_records': 0, 'removed_duplicates': 0}
            
            original_count = len(df)
            missing_cols = [col for col in unique_columns if col not in df.columns]
            if missing_cols:
                return {'status': 'error', 'file': object_path, 'message': f'Missing columns: {missing_cols}'}
            
            # Remove duplicates - keep first occurrence
            df_deduplicated = df.unique(subset=unique_columns, keep='first')
            final_count = len(df_deduplicated)
            removed_count = original_count - final_count
            
            if removed_count > 0:
                # Write back to MinIO (overwrites the original)
                if self.minio.write_parquet(df_deduplicated, object_path):
                    filename = object_path.split('/')[-1]
                    print(f"    SUCCESS: {filename}: Removed {removed_count:,} duplicates (kept {final_count:,}/{original_count:,} records)")
                else:
                    return {'status': 'error', 'file': object_path, 'message': 'Failed to write to MinIO'}
            
            return {
                'status': 'processed',
                'file': object_path,
                'original_records': original_count,
                'final_records': final_count,
                'removed_duplicates': removed_count,
                'had_duplicates': removed_count > 0
            }
            
        except Exception as e:
            return {'status': 'error', 'file': object_path, 'message': str(e), 'traceback': traceback.format_exc()}
    
    def check_and_remove_all_duplicates(self):
        """Check duplicates first, then remove them from MinIO."""
        print("CHECK AND REMOVE DUPLICATES IN MINIO")
        print("=" * 80)
        print("Processing: perpetual_orderBook -> perpetual_trades -> spot_trades -> perpetual_fundingRate -> perpetual_markPriceKlines -> indexPriceKlines")
        print()
        
        data_types = ['perpetual_orderBook', 'perpetual_trades', 'spot_trades', 'perpetual_fundingRate', 'perpetual_markPriceKlines', 'indexPriceKlines']
        start_time = datetime.now()
        
        for data_type in data_types:
            try:
                print(f"\n{'='*80}")
                print(f"CHECKING DUPLICATES: {data_type}")
                print(f"{'='*80}")
                
                # First check for duplicates
                check_results = self.process_files_for_data_type(data_type, 'check')
                files_with_dups = len([r for r in check_results if r.get('duplicate_records', 0) > 0])
                total_dups = sum(r.get('duplicate_records', 0) for r in check_results)
                
                if files_with_dups > 0:
                    print(f"\nREMOVING DUPLICATES: {data_type}")
                    print(f"Found {total_dups:,} duplicates in {files_with_dups} files. Removing now...")
                    
                    # Then remove duplicates
                    self.process_files_for_data_type(data_type, 'remove')
                    print(f"COMPLETED: {data_type} - Removed duplicates from {files_with_dups} files")
                else:
                    print(f"COMPLETED: {data_type} - No duplicates found")
                
            except KeyboardInterrupt:
                print(f"\nInterrupted by user during {data_type}")
                break
            except Exception as e:
                print(f"ERROR processing {data_type}: {str(e)}")
                continue
        
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"\nCHECK AND REMOVE COMPLETED! Duration: {duration}")
    

def main():
    """Main function - automatically check and remove duplicates from MinIO."""
    checker = DuplicateChecker()
    print("Starting automatic duplicate check and removal from MinIO")
    checker.check_and_remove_all_duplicates()

if __name__ == "__main__":
    main()