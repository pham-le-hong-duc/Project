"""
Silver merge: Hợp nhất dữ liệu aggregate (2_silver/aggregate) với funding rate (1_bronze/perpetual_fundingRate)
- Đầu vào: các file parquet aggregate theo interval (mặc định 5m)
- Đầu vào: funding rate theo tháng (parquet) ở bronze
- Đầu ra: datalake/2_silver/merge.csv
"""

from pathlib import Path
from typing import Optional, Dict, List
import argparse
import sys
import polars as pl


class SilverAggregateFundingMerger:
    def __init__(self,
                 datalake_path: str = "datalake",
                 interval: str = "all",
                 index_symbol: str = "btc-usdt",
                 perp_symbol: str = "btc-usdt-swap"):
        self.datalake = Path(datalake_path)
        self.interval = interval  # "all" hoặc một trong các: 5m, 15m, 1h, 4h, 1d
        self.index_symbol = index_symbol
        self.perp_symbol = perp_symbol
        self.spot_symbol = self.index_symbol  # spot symbol dùng trực tiếp, ví dụ: btc-usdt

        # Aggregate base dirs
        self.aggregate_base = {
            "indexPriceKlines": f"2_silver/aggregate/indexPriceKlines/{self.index_symbol}",
            "perpetual_markPriceKlines": f"2_silver/aggregate/perpetual_markPriceKlines/{self.perp_symbol}",
            "perpetual_orderBook":       f"2_silver/aggregate/perpetual_orderBook/{self.perp_symbol}",
            "perpetual_trades":          f"2_silver/aggregate/perpetual_trades/{self.perp_symbol}",
            "spot_trades":               f"2_silver/aggregate/spot_trades/{self.spot_symbol}",
        }

        # Bronze funding monthly parquet directory
        self.funding_dir = self.datalake / f"1_bronze/perpetual_fundingRate/{self.perp_symbol}"

    def _read_parquet_if_exists(self, rel_path: str) -> Optional[pl.DataFrame]:
        path = self.datalake / rel_path
        if not path.exists():
            print(f"⚠️  Missing: {path}")
            return None
        try:
            df = pl.read_parquet(path)
            if df.is_empty():
                return None
            # Chuẩn hóa timestamp_dt về microseconds
            if "timestamp_dt" in df.columns:
                df = df.with_columns(pl.col("timestamp_dt").dt.cast_time_unit("us"))
            return df.sort("timestamp_dt")
        except Exception as e:
            print(f"❌ Error reading {path}: {e}")
            return None

    def load_aggregate(self) -> Dict[str, pl.DataFrame]:
        out: Dict[str, pl.DataFrame] = {}
        intervals = ["5m", "15m", "1h", "4h", "1d"] if self.interval == "all" else [self.interval]
        for src_name, base_rel in self.aggregate_base.items():
            for itv in intervals:
                rel = f"{base_rel}/{itv}.parquet"
                df = self._read_parquet_if_exists(rel)
                if df is not None:
                    key = f"{src_name}_{itv}"
                    # Thêm prefix đầy đủ: {bảng}_{interval}_
                    cols = [c for c in df.columns if c != "timestamp_dt"]
                    df = df.rename({c: f"{key}_{c}" for c in cols})
                    out[key] = df
                    print(f"✓ {key}: {len(df)} rows, {len(df.columns)} cols")
        return out

    def load_funding(self) -> Optional[pl.DataFrame]:
        if not self.funding_dir.exists():
            print(f"⚠️  Funding dir not found: {self.funding_dir}")
            return None
        files = sorted(self.funding_dir.glob("*.parquet"))
        if not files:
            print(f"⚠️  No funding parquet files in {self.funding_dir}")
            return None
        dfs: List[pl.DataFrame] = []
        for f in files:
            try:
                d = pl.read_parquet(f)
                if not d.is_empty():
                    dfs.append(d)
            except Exception as e:
                print(f"❌ Error reading {f}: {e}")
        if not dfs:
            return None
        df = pl.concat(dfs, rechunk=True)
        # funding_time (ms) -> timestamp_dt (us)
        if "funding_time" in df.columns:
            df = df.with_columns(
                pl.from_epoch(pl.col("funding_time"), time_unit="ms")
                  .dt.cast_time_unit("us").alias("timestamp_dt")
            )
        df = df.sort("timestamp_dt")
        # Prefix các cột, bỏ cột instrument string nếu có
        cols = [c for c in df.columns if c != "timestamp_dt"]
        df = df.rename({c: f"perpetual_fundingRate_{c}" for c in cols})

        # Drop instrument name nếu có
        if "perpetual_fundingRate_instrument_name" in df.columns:
            df = df.drop("perpetual_fundingRate_instrument_name")
        print(f"✓ funding: {len(df)} rows, {len(df.columns)} cols")
        return df

    def merge(self) -> Optional[pl.DataFrame]:
        agg = self.load_aggregate()
        fund = self.load_funding()
        if not agg and fund is None:
            print("❌ Nothing to merge")
            return None
        # Khởi tạo merged từ một nguồn có sẵn
        merged: Optional[pl.DataFrame] = None
        keys = list(agg.keys())
        if keys:
            merged = agg[keys[0]]
        elif fund is not None:
            merged = fund
        # Join các aggregate còn lại
        for k in keys[1:]:
            merged = merged.join(agg[k], on="timestamp_dt", how="full", suffix="_dup")
            if "timestamp_dt_dup" in merged.columns:
                merged = merged.with_columns(
                    pl.coalesce([pl.col("timestamp_dt"), pl.col("timestamp_dt_dup")]).alias("timestamp_dt")
                ).drop("timestamp_dt_dup")
        # Join funding (left) và forward fill các cột funding
        if fund is not None:
            merged = merged.join(fund, on="timestamp_dt", how="left")
            fcols = [c for c in merged.columns if c.startswith("funding_")]
            if fcols:
                merged = merged.with_columns([pl.col(c).forward_fill() for c in fcols])
        merged = merged.sort("timestamp_dt")
        print(f"✓ merged: {len(merged)} rows, {len(merged.columns)} cols")
        return merged

    def save_csv(self, df: pl.DataFrame) -> Path:
        output_file = self.datalake / "2_silver" / "merge.csv"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df.write_csv(output_file)
        print(f"\n✅ Saved: {output_file}")
        return output_file


def main():
    parser = argparse.ArgumentParser(description="Merge 2_silver/aggregate với 1_bronze/perpetual_fundingRate và lưu CSV")
    parser.add_argument("--datalake-path", default="datalake", help="Path tới thư mục datalake")
    parser.add_argument("--interval", default="all", choices=["all", "5m", "15m", "1h", "4h", "1d"], help="Interval để lấy file aggregate (all = lấy tất cả)")
    parser.add_argument("--index-symbol", default="btc-usdt", help="Symbol index (indexPriceKlines)")
    parser.add_argument("--perp-symbol", default="btc-usdt-swap", help="Symbol perpetual (mark/orderbook/trades/funding)")

    args = parser.parse_args()

    merger = SilverAggregateFundingMerger(
        datalake_path=args.datalake_path,
        interval=args.interval,
        index_symbol=args.index_symbol,
        perp_symbol=args.perp_symbol,
    )

    df = merger.merge()
    if df is None or df.is_empty():
        print("❌ No data produced")
        sys.exit(1)
    merger.save_csv(df)
    sys.exit(0)


if __name__ == "__main__":
    main()
