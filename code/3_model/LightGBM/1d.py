"""
LightGBM Model Training - 1d Interval (Multiclass: 3 labels)

Walk-Forward Validation with Feature Selection for daily trading strategy.
Fixed test size with expanding train window approach.

WARNING: Very small dataset - results may have high variance.
"""

import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')
from datetime import datetime
import argparse


class LightGBMTrainer:
    """LightGBM Walk-Forward Validation for 1d interval"""
    
    def __init__(self, data_path: str = "datalake/4_diamond/1d.parquet"):
        self.data_path = Path(data_path)
        self.interval = "1d"
        
        # Walk-Forward Configuration for 1d
        self.test_size = 55             # ~2 months of daily data
        self.min_train_size = 90        # ~3 months (minimum train)
        self.step_size = 14             # ~2 weeks step
        
        # Model Configuration (adjusted for very small dataset)
        self.params = {
            'objective': 'multiclass',
            'num_class': 3,
            'metric': 'multi_logloss',
            'boosting_type': 'gbdt',
            'n_estimators': 300,        # Reduced
            'learning_rate': 0.1,       # Higher learning rate
            'num_leaves': 15,           # Simple trees
            'max_depth': 4,             # Shallow trees
            'subsample': 0.9,           # Use more data
            'colsample_bytree': 0.9,    # Use more features
            'min_child_samples': 5,     # Small minimum samples
            'reg_alpha': 0.1,           # L1 regularization
            'reg_lambda': 0.1,          # L2 regularization
            'random_state': 42,
            'n_jobs': -1,
            'verbose': -1
        }
        
        # Feature selection threshold - conservative
        self.feature_threshold = 'median'
        
        # Results storage
        self.results = []
        
        # Trading simulation parameter
        self.trading_fee = 0.0004  # 0.04% per trade
        
    def load_data(self):
        """Load and prepare data"""
        print(f"📖 Loading {self.interval} data from {self.data_path}")
        
        df = pd.read_parquet(self.data_path)
        print(f"✓ Loaded {len(df):,} rows, {len(df.columns)} columns")
        
        if len(df) < 200:
            print("🚨 CRITICAL WARNING: Very small dataset!")
        
        # Sort by timestamp
        df = df.sort_values('timestamp_dt').reset_index(drop=True)
        
        # Extract features and target
        target_col = f"label_{self.interval}"
        if target_col not in df.columns:
            raise ValueError(f"Target column {target_col} not found!")
        
        # Remove non-feature columns
        exclude_cols = ['timestamp_dt'] + [col for col in df.columns if col.startswith('label_')]
        feature_cols = [col for col in df.columns if col not in exclude_cols]
        
        X = df[feature_cols].copy()
        y = df[target_col].copy()
        timestamps = df['timestamp_dt'].copy()
        
        print(f"✓ Features: {len(feature_cols)}, Target: {target_col}")
        vc = y.value_counts(normalize=True).sort_index()
        dist_str = ", ".join([f"class {int(k)}: {v:.1%}" for k, v in vc.items()])
        print(f"✓ Target distribution: {dist_str}")
        print(f"🚨 Dataset size: {len(df)} rows")
        
        return X, y, timestamps
    
    def correlation_filter(self, X_train, X_test, threshold=0.95):
        """Remove highly correlated features before feature selection"""
        print(f"🧹 Correlation filter (removing features with correlation > {threshold})...")
        
        corr_matrix = X_train.corr().abs()
        upper_tri = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
        to_drop = [column for column in upper_tri.columns if any(upper_tri[column] > threshold)]
        print(f"✓ Removing {len(to_drop)} highly correlated features")
        keep_features = [col for col in X_train.columns if col not in to_drop]
        return X_train[keep_features], X_test[keep_features], keep_features
    
    def feature_selection(self, X_train, y_train, X_test):
        """Feature selection: LightGBM importance on correlation-filtered features"""
        print("🔧 LightGBM importance selection on correlation-filtered features...")
        
        train_data = lgb.Dataset(X_train, label=y_train)
        fs_params = self.params.copy()
        fs_params.update({'n_estimators': 80, 'learning_rate': 0.15, 'verbose': -1})
        model = lgb.train(
            fs_params,
            train_data,
            valid_sets=[train_data],
            callbacks=[lgb.early_stopping(20), lgb.log_evaluation(0)]
        )
        
        feature_importance = model.feature_importance(importance_type='gain')
        feature_names = X_train.columns
        importance_df = pd.DataFrame({'feature': feature_names, 'importance': feature_importance}).sort_values('importance', ascending=False)
        
        if self.feature_threshold == 'median':
            threshold_value = importance_df['importance'].median()
            selected_features = importance_df[importance_df['importance'] >= threshold_value]['feature'].tolist()
            if len(selected_features) < 20 and len(feature_names) >= 20:
                selected_features = importance_df.head(20)['feature'].tolist()
        else:
            n_features = int(self.feature_threshold)
            selected_features = importance_df.head(n_features)['feature'].tolist()
        
        print(f"✓ Final selection: {len(selected_features)} features")
        top_feature = importance_df.iloc[0]['feature'] if len(importance_df) > 0 else "N/A"
        return X_train[selected_features], X_test[selected_features], selected_features, top_feature
    
    def train_model(self, X_train, y_train, X_val=None, y_val=None):
        train_data = lgb.Dataset(X_train, label=y_train)
        valid_sets = [train_data]
        valid_names = ['train']
        if X_val is not None and y_val is not None:
            val_data = lgb.Dataset(X_val, label=y_val, reference=train_data)
            valid_sets.append(val_data)
            valid_names.append('val')
        model = lgb.train(
            self.params,
            train_data,
            valid_sets=valid_sets,
            valid_names=valid_names,
            callbacks=[lgb.early_stopping(40), lgb.log_evaluation(0)]
        )
        return model
    
    def evaluate_model(self, model, X_test, y_test):
        y_pred_proba = model.predict(X_test)
        y_pred = np.argmax(y_pred_proba, axis=1).astype(int)
        accuracy = (y_pred == y_test).mean()
        from sklearn.metrics import f1_score, classification_report
        macro_f1 = f1_score(y_test, y_pred, average='macro', zero_division=0)
        weighted_f1 = f1_score(y_test, y_pred, average='weighted', zero_division=0)
        report = classification_report(y_test, y_pred, digits=3)
        return {'accuracy': accuracy, 'macro_f1': macro_f1, 'weighted_f1': weighted_f1, 'report': report, 'y_pred': y_pred, 'y_pred_proba': y_pred_proba}
    
    def simulate_trading(self, y_test, y_pred, returns_test):
        """Simplified simulation: buy-only on class 2 signals"""
        trade_signals = (y_pred == 2).astype(int)
        actual_returns = returns_test if returns_test is not None else np.zeros(len(y_test))
        trade_returns = []
        total_trades = 0
        for i in range(len(trade_signals)):
            if trade_signals[i] == 1:
                ret = actual_returns[i] if i < len(actual_returns) else 0
                trade_return = ret - self.trading_fee
                trade_returns.append(trade_return)
                total_trades += 1
        if total_trades == 0:
            return {'total_pnl': 0, 'avg_trade_return': 0, 'total_trades': 0}
        total_pnl = sum(trade_returns)
        avg_trade_return = np.mean(trade_returns)
        return {'total_pnl': total_pnl, 'avg_trade_return': avg_trade_return, 'total_trades': total_trades}
    
    def walk_forward_validation(self):
        print(f"🚀 Starting Walk-Forward Validation for {self.interval}")
        print("="*60)
        X, y, timestamps = self.load_data()
        print("🧹 Pre-computing correlation filter on entire dataset...")
        X_corr_filtered, _, self.selected_features_corr = self.correlation_filter(X, X.iloc[:100])
        print(f"✓ Correlation filter: {len(X.columns)} → {len(X_corr_filtered.columns)} features")
        X = X_corr_filtered
        total_samples = len(X)
        print(f"📊 Total samples: {total_samples:,}")
        print(f"📊 Test size: {self.test_size:,}")
        print(f"📊 Min train size: {self.min_train_size:,}")
        print(f"📊 Step size: {self.step_size:,}")
        max_start_idx = total_samples - self.test_size
        start_indices = list(range(self.min_train_size, max_start_idx, self.step_size))
        if not start_indices:
            print("❌ Not enough data for walk-forward validation")
            return
        print(f"📊 Number of folds: {len(start_indices)}")
        print()
        fold_id = 0
        for train_end_idx in start_indices:
            fold_id += 1
            test_start_idx = train_end_idx
            test_end_idx = min(test_start_idx + self.test_size, total_samples)
            if test_end_idx - test_start_idx < self.test_size * 0.8:
                continue
            print(f"📁 Fold {fold_id}/{len(start_indices)}")
            X_train = X.iloc[:train_end_idx].copy()
            y_train = y.iloc[:train_end_idx].copy()
            X_test = X.iloc[test_start_idx:test_end_idx].copy()
            y_test = y.iloc[test_start_idx:test_end_idx].copy()
            train_start_time = timestamps.iloc[0]
            train_end_time = timestamps.iloc[train_end_idx-1]
            test_start_time = timestamps.iloc[test_start_idx]
            test_end_time = timestamps.iloc[test_end_idx-1]
            print(f"   📅 Train: {train_start_time} to {train_end_time} ({len(X_train):,} samples)")
            print(f"   📅 Test:  {test_start_time} to {test_end_time} ({len(X_test):,} samples)")
            X_train_selected, X_test_selected, selected_features, top_feature = self.feature_selection(X_train, y_train, X_test)
            print("   🤖 Training model...")
            model = self.train_model(X_train_selected, y_train)
            print("   📊 Evaluating...")
            metrics = self.evaluate_model(model, X_test_selected, y_test)
            result = {
                'fold_id': fold_id,
                'train_start': train_start_time,
                'train_end': train_end_time,
                'test_start': test_start_time,
                'test_end': test_end_time,
                'train_samples': len(X_train),
                'test_samples': len(X_test),
                'selected_features': len(selected_features),
                'accuracy': metrics['accuracy'],
                'macro_f1': metrics['macro_f1'],
                'weighted_f1': metrics['weighted_f1']
            }
            self.results.append(result)
            print(f"   ✓ Accuracy: {metrics['accuracy']:.3f}")
            print(f"   ✓ Macro-F1: {metrics['macro_f1']:.3f}")
            print(f"   ✓ Weighted-F1: {metrics['weighted_f1']:.3f}")
            print()
        self.print_summary()
        self.save_results()
        self.train_final_model(X, y)
    
    def print_summary(self):
        if not self.results:
            print("❌ No results to summarize")
            return
        print("="*60)
        print(f"📊 WALK-FORWARD VALIDATION SUMMARY - {self.interval}")
        print("="*60)
        accuracies = [r['accuracy'] for r in self.results]
        print(f"📈 Total Folds: {len(self.results)}")
        print(f"📈 Avg Accuracy: {np.mean(accuracies):.3f} ± {np.std(accuracies):.3f}")
        print(f"📈 Best Accuracy: {np.max(accuracies):.3f}")
        print(f"📈 Worst Accuracy: {np.min(accuracies):.3f}")
    
    def save_results(self):
        if not self.results:
            return
        results_df = pd.DataFrame(self.results)
        output_dir = Path("results/lightgbm")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"training_log_{self.interval}.csv"
        results_df.to_csv(output_file, index=False)
        print(f"💾 Results saved to: {output_file}")
        summary_file = output_dir / f"summary_{self.interval}.txt"
        with open(summary_file, 'w') as f:
            f.write(f"LightGBM Walk-Forward Validation Summary - {self.interval}\n")
            f.write(f"Generated: {datetime.now()}\n\n")
            accuracies = [r['accuracy'] for r in self.results]
            f.write(f"Total Folds: {len(self.results)}\n")
            f.write(f"Avg Accuracy: {np.mean(accuracies):.3f} ± {np.std(accuracies):.3f}\n")
            f.write(f"Best Accuracy: {np.max(accuracies):.3f}\n")
            f.write(f"Worst Accuracy: {np.min(accuracies):.3f}\n")
        print(f"📄 Summary saved to: {summary_file}")
    
    def train_final_model(self, X, y):
        print("🎯 Training final model on all data...")
        X_selected, _, selected_features, _ = self.feature_selection(X, y, X.iloc[:100])
        final_model = self.train_model(X_selected, y)
        output_dir = Path("models/lightgbm")
        output_dir.mkdir(parents=True, exist_ok=True)
        model_file = output_dir / f"final_model_{self.interval}.txt"
        final_model.save_model(str(model_file))
        features_file = output_dir / f"selected_features_{self.interval}.txt"
        with open(features_file, 'w') as f:
            for feature in selected_features:
                f.write(f"{feature}\n")
        print(f"💾 Final model saved to: {model_file}")
        print(f"💾 Selected features saved to: {features_file}")
        print(f"✓ Final model uses {len(selected_features)} features")
    
    def run(self):
        try:
            self.walk_forward_validation()
            return True
        except Exception as e:
            print(f"❌ Error during training: {str(e)}")
            import traceback
            traceback.print_exc()
            return False


def main():
    parser = argparse.ArgumentParser(description=f'LightGBM Walk-Forward Validation for 1d interval')
    parser.add_argument('--data-path', default='datalake/4_diamond/1d.parquet', help='Path to training data')
    args = parser.parse_args()
    print(f"🚀 Starting LightGBM Training for 1d interval")
    print(f"📁 Data path: {args.data_path}")
    print()
    trainer = LightGBMTrainer(data_path=args.data_path)
    success = trainer.run()
    if success:
        print("✅ Training completed successfully!")
    else:
        print("❌ Training failed!")
        sys.exit(1)


if __name__ == "__main__":
    import sys
    main()
