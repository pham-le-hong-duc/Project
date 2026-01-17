"""
TimescaleDB Client for Trading Data

Provides connection and helper methods for TimescaleDB operations.
- Create/update tables with hypertables
- Upsert data with automatic conflict resolution
- Query helper methods
"""
import psycopg2
from psycopg2.extras import execute_values
import polars as pl
from typing import Optional, List
import os


class TimescaleDBClient:
    """TimescaleDB client for trading data management."""
    
    def __init__(self, 
                 host: str = None,
                 port: int = None,
                 database: str = None,
                 user: str = None,
                 password: str = None):
        """
        Initialize TimescaleDB connection.
        
        Args:
            host: TimescaleDB host (default: from env or localhost)
            port: TimescaleDB port (default: from env or 5432)
            database: Database name (default: from env or okx)
            user: Username (default: from env or okx_user)
            password: Password (default: from env or okx_password)
        """
        # Get connection info from environment or defaults
        self.host = host or os.getenv('TIMESCALE_HOST', 'localhost')
        self.port = port or int(os.getenv('TIMESCALE_PORT', '5432'))
        self.database = database or os.getenv('TIMESCALE_DB', 'okx')
        self.user = user or os.getenv('TIMESCALE_USER', 'okx_user')
        self.password = password or os.getenv('TIMESCALE_PASSWORD', 'okx_password')
        
        # Connect to TimescaleDB
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
        
        # Enable autocommit for convenience
        self.conn.autocommit = True
        
        print(f"Connected to TimescaleDB: {self.host}:{self.port}/{self.database}")
    
    def execute(self, query: str, params: tuple = None):
        """
        Execute SQL query.
        
        Args:
            query: SQL query string
            params: Query parameters
        
        Returns:
            Query result
        """
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            try:
                return cur.fetchall()
            except:
                return None
    
    def query_to_df(self, query: str) -> pl.DataFrame:
        """
        Execute SQL query and return Polars DataFrame.
        
        Args:
            query: SQL query string
        
        Returns:
            Polars DataFrame
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            data = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return pl.DataFrame(data, schema=columns)
    
    def list_tables(self) -> List[str]:
        """
        List all tables in current database.
        
        Returns:
            List of table names
        """
        result = self.execute("""
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public'
            ORDER BY tablename
        """)
        return [row[0] for row in result]
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists.
        
        Args:
            table_name: Name of the table
        
        Returns:
            True if table exists, False otherwise
        """
        result = self.execute("""
            SELECT EXISTS (
                SELECT FROM pg_tables 
                WHERE schemaname = 'public' 
                AND tablename = %s
            )
        """, (table_name,))
        return result[0][0] if result else False
    
    def get_table_info(self, table_name: str):
        """
        Get table information (schema, row count).
        
        Args:
            table_name: Table name
        """
        if not self.table_exists(table_name):
            print(f"Table '{table_name}' does not exist")
            return
        
        # Get row count
        count = self.execute(f"SELECT COUNT(*) FROM {table_name}")[0][0]
        
        # Get schema
        schema = self.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))
        
        print(f"\nTable: {table_name}")
        print(f"Rows: {count:,}")
        print("\nSchema:")
        for col in schema:
            print(f"  {col[0]}: {col[1]}")
    
    def drop_table(self, table_name: str):
        """
        Drop table if exists.
        
        Args:
            table_name: Table name to drop
        """
        try:
            self.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
            print(f"Dropped table '{table_name}'")
            return True
        except Exception as e:
            print(f"Error dropping table '{table_name}': {e}")
            return False
    
    def get_existing_timestamps(self, table_name: str, target_date=None) -> set:
        """
        Get existing timestamps from a table, optionally filtered by date.
        
        Args:
            table_name: Name of the table
            target_date: Optional datetime to filter by specific date (gets that day's data only)
        
        Returns:
            Set of existing ts_ms values
        """
        try:
            if target_date:
                # Use timestamp_dt column for date filtering (more reliable than ts_ms range)
                date_str = target_date.strftime('%Y-%m-%d')
                result = self.execute(
                    f"SELECT ts_ms FROM {table_name} WHERE DATE(timestamp_dt) = %s",
                    (date_str,)
                )
            else:
                result = self.execute(f"SELECT ts_ms FROM {table_name}")
            
            return set([row[0] for row in result])
        except Exception as e:
            print(f"Warning: Could not fetch timestamps from '{table_name}': {e}")
            return set()
    
    def get_complete_timestamps(self, table_name: str, target_date=None) -> set:
        """
        Get timestamps of complete rows (where row was actually aggregated).
        
        For most tables, we just check if the row exists (not NULL checks).
        This is because NULL values can be valid for many aggregated features
        (e.g., no trades in window, only buy trades, only sell trades, etc.)
        
        Args:
            table_name: Name of the table
            target_date: Optional datetime to filter by specific date (gets that day's data only)
        
        Returns:
            Set of ts_ms values that exist in the table
        """
        # For now, just use regular timestamps (no NULL filtering)
        # The aggregation logic already skips windows with trade_count=0
        return self.get_existing_timestamps(table_name, target_date)
    
    def create_table_from_dataframe(self, df: pl.DataFrame, table_name: str,
                                   time_column: str = "ts_ms",
                                   replace: bool = False):
        """
        Create table from Polars DataFrame with TimescaleDB hypertable.
        
        Args:
            df: Polars DataFrame
            table_name: Name of table to create
            time_column: Time column for hypertable partitioning
            replace: If True, drop existing table first
        """
        try:
            if replace and self.table_exists(table_name):
                self.drop_table(table_name)
            
            # Map Polars types to PostgreSQL types
            type_mapping = {
                pl.Int64: "BIGINT",
                pl.Int32: "INTEGER",
                pl.Float64: "DOUBLE PRECISION",
                pl.Float32: "REAL",
                pl.Utf8: "TEXT",
                pl.Boolean: "BOOLEAN",
                pl.Datetime: "TIMESTAMP",
            }
            
            # Build column definitions
            columns = []
            for col_name, dtype in zip(df.columns, df.dtypes):
                pg_type = type_mapping.get(dtype, "TEXT")
                # Quote column names to handle special characters and numbers
                quoted_col = f'"{col_name}"'
                # Add PRIMARY KEY to time column
                if col_name == time_column:
                    columns.append(f"{quoted_col} {pg_type} PRIMARY KEY")
                else:
                    columns.append(f"{quoted_col} {pg_type}")
            
            columns_sql = ",\n    ".join(columns)
            
            # Create table
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {columns_sql}
            )
            """
            self.execute(create_sql)
            
            # Convert to hypertable for time-series optimization
            try:
                self.execute(f"""
                    SELECT create_hypertable('{table_name}', '{time_column}', 
                                            chunk_time_interval => 86400000,
                                            if_not_exists => TRUE)
                """)
                print(f"Created hypertable '{table_name}'")
            except Exception as e:
                print(f"Created table '{table_name}' (hypertable conversion skipped: {e})")
            
            return True
            
        except Exception as e:
            print(f"Error creating table '{table_name}': {e}")
            return False
    
    def upsert_dataframe(self, df: pl.DataFrame, table_name: str,
                        key_column: str = "ts_ms") -> int:
        """
        Upsert DataFrame into TimescaleDB table.
        
        Uses INSERT ... ON CONFLICT for efficient upsert.
        
        Args:
            df: Polars DataFrame to upsert
            table_name: Target table name
            key_column: Column to use for conflict detection (default: ts_ms)
        
        Returns:
            Number of rows affected
        """
        if df.is_empty():
            print(f"No data to upsert into '{table_name}'")
            return 0
        
        try:
            # Auto-create table if not exists
            if not self.table_exists(table_name):
                print(f"Table '{table_name}' does not exist. Creating...")
                self.create_table_from_dataframe(df.head(0), table_name, time_column=key_column)
            
            # Convert DataFrame to list of tuples
            data = [tuple(row) for row in df.iter_rows()]
            columns = df.columns
            
            # Build upsert SQL with quoted column names
            columns_sql = ", ".join([f'"{col}"' for col in columns])
            placeholders = ", ".join(["%s"] * len(columns))
            update_columns = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns if col != key_column])
            
            upsert_sql = f"""
                INSERT INTO {table_name} ({columns_sql})
                VALUES %s
                ON CONFLICT ("{key_column}") 
                DO UPDATE SET {update_columns}
            """
            
            # Execute batch insert
            with self.conn.cursor() as cur:
                execute_values(cur, upsert_sql, data, template=f"({placeholders})")
            
            rows_inserted = len(df)
            return rows_inserted
            
        except Exception as e:
            print(f"Error upserting into '{table_name}': {e}")
            raise
    
    def close(self):
        """Close database connection."""
        self.conn.close()
        print("TimescaleDB connection closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def initialize_okx_tables(self, force_recreate=False):
        """
        Initialize OKX tables with hypertables.
        
        Creates 26 tables:
        - 25 timeframe tables (5 data types × 5 timeframes)
        - 1 perpetual_fundingRate table
        
        Args:
            force_recreate: If True, drop and recreate tables. If False, only create if not exists.
        """
        print("="*60)
        print("INITIALIZING OKX TABLES IN TIMESCALEDB")
        print("="*60)
        
        # Check existing tables
        existing_tables = self.list_tables()
        print(f"\nExisting tables: {len(existing_tables)}")
        
        if len(existing_tables) >= 26 and not force_recreate:
            print("✓ Tables already initialized. Skipping...")
            print("  Use force_recreate=True to recreate tables")
            return True
        
        if force_recreate and existing_tables:
            print("\nDropping existing tables...")
            for table in existing_tables:
                self.drop_table(table)
            print(f"  ✓ Dropped {len(existing_tables)} tables")
        
        data_types = ['spot_trades', 'perpetual_trades', 'perpetual_orderBook',
                      'indexPriceKlines', 'perpetual_markPriceKlines']
        timeframes = ['5m', '15m', '1h', '4h', '1d']
        
        created_count = 0
        skipped_count = 0
        
        # 1. Create perpetual_fundingRate table
        print("\n1. Creating perpetual_fundingRate...")
        if 'perpetual_fundingRate' in existing_tables and not force_recreate:
            print("  ⊙ perpetual_fundingRate (already exists)")
            skipped_count += 1
        else:
            try:
                self.execute("""
                    CREATE TABLE IF NOT EXISTS perpetual_fundingRate (
                        instrument_name TEXT,
                        funding_rate DOUBLE PRECISION,
                        funding_time BIGINT,
                        PRIMARY KEY (instrument_name, funding_time)
                    )
                """)
                self.execute("""
                    SELECT create_hypertable('perpetual_fundingRate', 'funding_time',
                                            chunk_time_interval => 86400000,
                                            if_not_exists => TRUE)
                """)
                created_count += 1
                print("  ✓ perpetual_fundingRate")
            except Exception as e:
                print(f"  ✗ perpetual_fundingRate: {e}")
        
        # 2. Create timeframe tables
        print("\n2. Creating timeframe tables...")
        
        for data_type in data_types:
            print(f"\n{data_type}:")
            
            for timeframe in timeframes:
                table_name = f"{data_type}_{timeframe}"
                
                if table_name in existing_tables and not force_recreate:
                    print(f"  ⊙ {table_name} (already exists)")
                    skipped_count += 1
                else:
                    try:
                        self.execute(f"""
                            CREATE TABLE IF NOT EXISTS {table_name} (
                                timestamp_dt TIMESTAMP,
                                ts_ms BIGINT PRIMARY KEY
                            )
                        """)
                        self.execute(f"""
                            SELECT create_hypertable('{table_name}', 'ts_ms',
                                                    chunk_time_interval => 86400000,
                                                    if_not_exists => TRUE)
                        """)
                        created_count += 1
                        print(f"  ✓ {table_name}")
                    except Exception as e:
                        print(f"  ✗ {table_name}: {e}")
        
        print(f"\n{'='*60}")
        total_expected = len(data_types) * len(timeframes) + 1
        if force_recreate:
            print(f"Recreated: {created_count}/{total_expected} tables")
        else:
            print(f"Created: {created_count}, Skipped: {skipped_count}, Total: {created_count + skipped_count}/{total_expected}")
        print(f"{'='*60}")
        
        # List all tables
        tables = self.list_tables()
        print(f"\nTotal tables: {len(tables)}")
        
        return created_count == total_expected


# Example usage
if __name__ == "__main__":
    # Initialize client
    with TimescaleDBClient() as ts:
        # List tables
        tables = ts.list_tables()
        print(f"\nTables: {tables}")
        
        # Initialize OKX tables (uncomment to create tables)
        # ts.initialize_okx_tables()
