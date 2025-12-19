"""
Gold Layer - Stage 4: Create 3-class Labels

Create 3-class labels per interval using Index Price close:
- 2: Return > +Threshold (strong up)
- 1: Return < -Threshold (strong down)
- 0: Otherwise (small move)

Thresholds per interval (absolute return):
- 5m:  0.0018 (0.18%)
- 15m: 0.0035 (0.35%)
- 1h:  0.0065 (0.65%)
- 4h:  0.0120 (1.20%)
- 1d:  0.0250 (2.50%)
"""

import polars as pl
from pathlib import Path
import argparse
import sys


class LabelCreator:
    """Create 3-class labels for multi-timeframe prediction."""

    def __init__(self, datalake_path: str = "datalake"):
        self.datalake_path = Path(datalake_path)

        # Input and output paths
        self.input_file = self.datalake_path / "3_gold" / "step_3.parquet"
        self.output_file = self.datalake_path / "3_gold" / "step_4.parquet"

        # Define intervals and their corresponding periods for label creation
        self.intervals = ["5m", "15m", "1h", "4h", "1d"]

        # Base frequency is 5m (each row is one 5m bar). Horizon is 1 period of each interval.
        # So the shift (in rows) for each label is:
        #   5m -> 1 step, 15m -> 3 steps, 1h -> 12 steps, 4h -> 48 steps, 1d -> 288 steps
        self.label_periods = {
            "5m": 1,
            "15m": 3,
            "1h": 12,
            "4h": 48,
            "1d": 288,
        }

        # Define price columns to use for label creation (use Index Price close)
        self.price_columns = {
            "5m": "index_price_5m_close",
            "15m": "index_price_15m_close",
            "1h": "index_price_1h_close",
            "4h": "index_price_4h_close",
            "1d": "index_price_1d_close",
        }

    def load_data(self) -> pl.DataFrame:
        """Load input data from step_3.parquet"""
        if not self.input_file.exists():
            raise FileNotFoundError(f"Input file not found: {self.input_file}")

        print(f"Loading data from {self.input_file}")
        df = pl.read_parquet(self.input_file)
        print(f"Loaded {len(df):,} rows, {len(df.columns)} columns")
        return df

    def create_three_class_labels(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create 3-class labels per interval using index price close and dynamic thresholds.
        Labels: 2 (up > +alpha*vol), 1 (down < -alpha*vol), 0 (between -alpha*vol and +alpha*vol).
        Also creates and retains forward return and rolling volatility columns per interval.
        """
        print("Creating 3-class labels using dynamic thresholds (rolling volatility)...")
        # Sort by time to ensure correct shifting
        df = df.sort("timestamp_dt")

        # Horizon (in 5m rows) per interval is defined in self.label_periods
        # Create forward return columns for each interval first
        ret_exprs = []
        for interval in self.intervals:
            price_col = self.price_columns.get(interval)
            if price_col not in df.columns:
                print(f"Warning: Price column {price_col} not found, skipping {interval}")
                continue
            h = self.label_periods[interval]
            ret_col = f"ret_fwd_{interval}"
            ret_expr = (
                ((pl.col(price_col).shift(-h) - pl.col(price_col)) / (pl.col(price_col) + 1e-9))
                .alias(ret_col)
            )
            ret_exprs.append(ret_expr)
        if ret_exprs:
            df = df.with_columns(ret_exprs)
            print(f"Created {len(ret_exprs)} forward return columns")

        # Create volatility columns (rolling std of forward return shifted by 1 to avoid leakage)
        vol_exprs = []
        window = 20
        for interval in self.intervals:
            ret_col = f"ret_fwd_{interval}"
            if ret_col not in df.columns:
                continue
            vol_col = f"vol_{interval}"
            vol_expr = (
                pl.col(ret_col)
                .shift(1)
                .rolling_std(window_size=window)
                .alias(vol_col)
            )
            vol_exprs.append(vol_expr)
        if vol_exprs:
            df = df.with_columns(vol_exprs)
            print(f"Created {len(vol_exprs)} rolling volatility columns (window={window})")

        # Alphas per interval
        alphas = {
            "5m": 0.5,
            "15m": 0.6,
            "1h": 0.7,
            "4h": 0.8,
            "1d": 1.0,
        }

        # Create labels using dynamic threshold alpha * vol
        label_exprs = []
        for interval in self.intervals:
            ret_col = f"ret_fwd_{interval}"
            vol_col = f"vol_{interval}"
            if ret_col not in df.columns or vol_col not in df.columns:
                continue
            alpha = alphas[interval]
            label_expr = (
                pl.when(pl.col(ret_col).is_not_null() & pl.col(vol_col).is_not_null())
                .then(
                    pl.when(pl.col(ret_col) > alpha * pl.col(vol_col)).then(2)
                    .when(pl.col(ret_col) < -alpha * pl.col(vol_col)).then(1)
                    .otherwise(0)
                )
                .otherwise(None)
                .cast(pl.Int8)
                .alias(f"label_{interval}")
            )
            label_exprs.append(label_expr)
        if label_exprs:
            df = df.with_columns(label_exprs)
            print(f"Created {len(label_exprs)} 3-class label columns with dynamic thresholds")
        return df

    def create_all_labels(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create the requested 3-class labels only."""
        return self.create_three_class_labels(df)

    def analyze_label_distribution(self, df: pl.DataFrame) -> None:
        """Analyze 3-class label distributions (0/1/2) per interval using rows sampled at the interval stride.
        Example: 15m uses every 3rd row, 1h every 12th row, etc.
        """
        print("\n3-class Label Distribution Analysis:")
        # Ensure time order and add a temporary row index to sample by stride
        df_sorted = df.sort("timestamp_dt").with_columns(
            pl.arange(0, pl.len()).alias("_row_id")
        )
        for interval in self.intervals:
            label_col = f"label_{interval}"
            if label_col not in df_sorted.columns:
                continue
            stride = self.label_periods[interval]
            df_int = df_sorted.filter((pl.col("_row_id") % stride) == 0)
            counts = df_int.select([
                pl.col(label_col).eq(0).sum().alias("c0"),
                pl.col(label_col).eq(1).sum().alias("c1"),
                pl.col(label_col).eq(2).sum().alias("c2"),
                pl.col(label_col).is_null().sum().alias("nulls"),
                pl.col(label_col).count().alias("total"),
            ]).to_dict(as_series=False)
            c0, c1, c2 = counts["c0"][0], counts["c1"][0], counts["c2"][0]
            total = counts["total"][0]
            nulls = counts["nulls"][0]
            valid = max(total - nulls, 1)
            print(f"  {interval}: 0={c0:,} ({c0/valid:.1%}), 1={c1:,} ({c1/valid:.1%}), 2={c2:,} ({c2/valid:.1%}) | total={total:,}, nulls={nulls:,}")

    def save_data(self, df: pl.DataFrame) -> None:
        """Save labeled data to step_4.parquet"""
        output_dir = self.output_file.parent
        output_dir.mkdir(parents=True, exist_ok=True)

        print(f"Saving labeled data to {self.output_file}")
        df.write_parquet(self.output_file, compression="snappy")

        # Count new features
        try:
            original_cols = len(pl.read_parquet(self.input_file).columns)
        except Exception:
            original_cols = len(df.columns)
        new_cols = len(df.columns) - original_cols

        print(f"Saved {len(df):,} rows. Columns: original={original_cols}, new={new_cols}, total={len(df.columns)}")
        if not df.is_empty():
            min_time = df.select(pl.col("timestamp_dt").min()).item()
            max_time = df.select(pl.col("timestamp_dt").max()).item()
            print(f"Time range: {min_time} to {max_time}")

    def run(self) -> bool:
        """Run the label creation process."""
        try:
            df = self.load_data()
            df_labeled = self.create_all_labels(df)
            self.analyze_label_distribution(df_labeled)
            self.save_data(df_labeled)
            return True
        except Exception as e:
            print(f"Error: {e}")
            return False


def main():
    parser = argparse.ArgumentParser(description="Create 3-class labels for multi-timeframe prediction")
    parser.add_argument("--datalake-path", default="datalake", help="Path to datalake directory")

    args = parser.parse_args()

    creator = LabelCreator(datalake_path=args.datalake_path)
    success = creator.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
