"""
MinIO Helper for WebSocket Streams
Handles writing Parquet files directly to MinIO S3
"""
import io
import os
from minio import Minio
from minio.error import S3Error
import polars as pl


class MinIOWriter:
    """Helper class to write Parquet files to MinIO"""
    
    def __init__(self, 
                 endpoint=None,
                 access_key=None,
                 secret_key=None,
                 bucket="okx",
                 secure=False):
        """
        Initialize MinIO client
        
        Args:
            endpoint: MinIO endpoint (default: localhost:9000)
            access_key: MinIO access key (default: admin)
            secret_key: MinIO secret key (default: password)
            bucket: Bucket name (default: okx)
            secure: Use HTTPS if True
        """
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = access_key or os.getenv("MINIO_ACCESS_KEY", "admin")
        self.secret_key = secret_key or os.getenv("MINIO_SECRET_KEY", "password")
        self.bucket = bucket or os.getenv("MINIO_BUCKET", "okx")
        self.secure = secure
        
        # Initialize MinIO client
        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure
        )
        
        # Ensure bucket exists
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
                print(f"Created MinIO bucket: {self.bucket}")
        except S3Error as e:
            print(f"MinIO bucket check/create error: {e}")
    
    def write_parquet(self, df, object_path):
        """
        Write Polars DataFrame to MinIO as Parquet
        
        Args:
            df: Polars DataFrame
            object_path: Object path in MinIO (e.g., 'spot_trades/btc-usdt/2025-01-01.parquet')
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert DataFrame to Parquet in memory
            buffer = io.BytesIO()
            df.write_parquet(buffer)
            buffer.seek(0)
            
            # Upload to MinIO
            self.client.put_object(
                self.bucket,
                object_path,
                buffer,
                length=buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
            
            return True
            
        except S3Error as e:
            print(f"MinIO write error for {object_path}: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error writing to MinIO: {e}")
            return False
    
    def read_parquet(self, object_path):
        """
        Read Parquet file from MinIO
        
        Args:
            object_path: Object path in MinIO
        
        Returns:
            Polars DataFrame or None if not exists
        """
        try:
            response = self.client.get_object(self.bucket, object_path)
            data = response.read()
            response.close()
            response.release_conn()
            
            buffer = io.BytesIO(data)
            return pl.read_parquet(buffer)
            
        except S3Error as e:
            if e.code == 'NoSuchKey':
                return None
            print(f"MinIO read error for {object_path}: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error reading from MinIO: {e}")
            return None
    
    def object_exists(self, object_path):
        """
        Check if object exists in MinIO
        
        Args:
            object_path: Object path to check
        
        Returns:
            True if exists, False otherwise
        """
        try:
            self.client.stat_object(self.bucket, object_path)
            return True
        except S3Error as e:
            if e.code == 'NoSuchKey':
                return False
            print(f"MinIO stat error for {object_path}: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error checking MinIO object: {e}")
            return False
    
    def list_objects(self, prefix='', recursive=True):
        """
        List objects in MinIO with given prefix
        
        Args:
            prefix: Object prefix to filter (e.g., 'spot_trades/btc-usdt/')
            recursive: List recursively if True
        
        Returns:
            List of object names
        """
        try:
            objects = self.client.list_objects(
                self.bucket,
                prefix=prefix,
                recursive=recursive
            )
            return [obj.object_name for obj in objects]
        except S3Error as e:
            print(f"MinIO list error for prefix '{prefix}': {e}")
            return []
        except Exception as e:
            print(f"Unexpected error listing MinIO objects: {e}")
            return []
    
    def delete_object(self, object_path):
        """
        Delete object from MinIO
        
        Args:
            object_path: Object path to delete
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.client.remove_object(self.bucket, object_path)
            return True
        except S3Error as e:
            print(f"MinIO delete error for {object_path}: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error deleting from MinIO: {e}")
            return False
