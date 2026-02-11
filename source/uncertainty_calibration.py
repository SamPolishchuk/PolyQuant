"""
Polymarket Probability Calibration Model
=========================================

This script analyzes the calibration of Polymarket probabilities over time.
It builds a model that predicts the true probability of an event occurring
given the displayed probability, time remaining until resolution, and market liquidity.

Model: XGBoost Gradient Boosting
- Handles non-linear relationships between time and calibration
- Captures interactions between probability levels and time horizons
- Incorporates liquidity signals for accuracy assessment
- Robust to varying market dynamics

WHY LIQUIDITY MATTERS:
1. Information Quality: High liquidity = more traders = better price discovery
2. Bid-Ask Spreads: Liquid markets have tighter spreads, more accurate prices
3. Manipulation Resistance: High volume markets harder to manipulate
4. Staleness: Low liquidity may mean prices don't update with new information

LIQUIDITY FEATURES USED:
- Total market volume (cumulative USD traded)
- Recent trading activity (volume in last N days)
- Trade velocity (trades per day)
- Volume concentration (how evenly distributed is trading)
- Liquidity regime (high/medium/low based on percentiles)

Requirements:
    pip install pandas pyarrow xgboost scikit-learn matplotlib seaborn tqdm
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pyarrow.parquet as pq
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# ML libraries
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, log_loss
from sklearn.calibration import calibration_curve

# Visualization
import matplotlib.pyplot as plt
import seaborn as sns

# Progress tracking
from tqdm import tqdm

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)


class PolymarketCalibration:
    """
    Analyzes and models the calibration of Polymarket probabilities.
    
    Calibration = How well do displayed probabilities match actual outcomes?
    For example: Of all times Polymarket showed 70%, did events occur 70% of the time?
    """
    
    def __init__(self, parquet_path: str):
        """
        Initialize the calibration analyzer.
        
        Args:
            parquet_path: Path to the quant.parquet file
        """
        self.parquet_path = parquet_path
        self.df = None
        self.market_outcomes = None
        self.calibration_data = None
        self.model = None
        
    def load_data(self, train_start: str, train_end: str, 
                  test_start: str, test_end: str,
                  chunk_size: int = 1_000_000):
        """
        Load data in chunks to handle the 30GB file efficiently.
        
        Args:
            train_start: Start date for training data (format: 'YYYY-MM-DD')
            train_end: End date for training data
            test_start: Start date for testing data
            test_end: End date for testing data
            chunk_size: Number of rows to process at once
        """
        print("=" * 80)
        print("LOADING POLYMARKET DATA")
        print("=" * 80)
        
        # Convert date strings to timestamps
        train_start_ts = int(pd.Timestamp(train_start).timestamp())
        train_end_ts = int(pd.Timestamp(train_end).timestamp())
        test_start_ts = int(pd.Timestamp(test_start).timestamp())
        test_end_ts = int(pd.Timestamp(test_end).timestamp())
        
        print(f"\nTraining period: {train_start} to {train_end}")
        print(f"Testing period:  {test_start} to {test_end}")
        
        # Read parquet file
        parquet_file = pq.ParquetFile(self.parquet_path)
        
        # Columns we need (expanded for liquidity analysis)
        columns = ['timestamp', 'datetime', 'event_id', 'market_id', 'question',
                   'price', 'usd_amount', 'token_amount', 'maker_direction', 
                   'taker_direction', 'nonusdc_side', 'transaction_hash']
        
        chunks = []
        total_rows = 0
        
        print("\nReading data in chunks...")
        for batch in tqdm(parquet_file.iter_batches(batch_size=chunk_size, 
                                                     columns=columns)):
            df_chunk = batch.to_pandas()
            
            # Filter by date range (both train and test periods)
            mask = ((df_chunk['timestamp'] >= train_start_ts) & 
                    (df_chunk['timestamp'] <= train_end_ts)) | \
                   ((df_chunk['timestamp'] >= test_start_ts) & 
                    (df_chunk['timestamp'] <= test_end_ts))
            
            df_filtered = df_chunk[mask].copy()
            
            if len(df_filtered) > 0:
                chunks.append(df_filtered)
                total_rows += len(df_filtered)
        
        if not chunks:
            raise ValueError("No data found in the specified date ranges!")
        
        self.df = pd.concat(chunks, ignore_index=True)
        print(f"\nLoaded {total_rows:,} transactions")
        print(f"Unique markets: {self.df['market_id'].nunique():,}")
        print(f"Date range: {self.df['datetime'].min()} to {self.df['datetime'].max()}")
        
        # Add period label
        self.df['period'] = 'train'
        test_mask = ((self.df['timestamp'] >= test_start_ts) & 
                     (self.df['timestamp'] <= test_end_ts))
        self.df.loc[test_mask, 'period'] = 'test'
        
        print(f"Training samples: {(self.df['period'] == 'train').sum():,}")
        print(f"Testing samples:  {(self.df['period'] == 'test').sum():,}")
        
        return self
    
    def get_market_outcomes(self, polymarket_api_or_heuristic='heuristic'):
        """
        Determine the final outcome (YES/NO) for each market.
        
        This is critical: we need to know which markets resolved to YES vs NO
        to measure calibration.
        
        Args:
            polymarket_api_or_heuristic: 'api' to fetch from Polymarket API (requires implementation),
                                         'heuristic' to infer from final prices
        """
        print("\n" + "=" * 80)
        print("DETERMINING MARKET OUTCOMES")
        print("=" * 80)
        
        market_data = []
        
        for market_id in tqdm(self.df['market_id'].unique(), desc="Processing markets"):
            market_df = self.df[self.df['market_id'] == market_id].copy()
            market_df = market_df.sort_values('timestamp')
            
            # Get market metadata
            event_id = market_df['event_id'].iloc[0]
            question = market_df['question'].iloc[0]
            first_trade_time = market_df['timestamp'].min()
            last_trade_time = market_df['timestamp'].max()
            total_volume = market_df['usd_amount'].sum()
            
            # Heuristic: final outcome based on last 100 trades
            last_trades = market_df.tail(100)
            final_price = last_trades['price'].mean()
            
            # Assume outcome: YES if final price > 0.9, NO if < 0.1, UNKNOWN otherwise
            if final_price > 0.9:
                outcome = 1  # YES
            elif final_price < 0.1:
                outcome = 0  # NO
            else:
                outcome = None  # UNKNOWN (market might still be active or ambiguous)
            
            market_data.append({
                'market_id': market_id,
                'event_id': event_id,
                'question': question,
                'first_trade_time': first_trade_time,
                'last_trade_time': last_trade_time,
                'total_volume': total_volume,
                'final_price': final_price,
                'outcome': outcome
            })
        
        self.market_outcomes = pd.DataFrame(market_data)
        
        # Filter to markets with known outcomes
        known_outcomes = self.market_outcomes['outcome'].notna().sum()
        print(f"\nMarkets with known outcomes: {known_outcomes:,} / {len(self.market_outcomes):,}")
        print(f"YES outcomes: {(self.market_outcomes['outcome'] == 1).sum():,}")
        print(f"NO outcomes:  {(self.market_outcomes['outcome'] == 0).sum():,}")
        
        return self
    
    def create_calibration_dataset(self, time_buckets_days=[1, 3, 7, 14, 30, 60, 90, 180],
                                   lookback_windows=[1, 7, 30]):
        """
        Create the dataset for training the calibration model.
        
        For each trade, we calculate:
        - Displayed probability (price)
        - Time remaining until market close
        - Liquidity metrics (total volume, recent activity, trade velocity)
        - Actual outcome (did it resolve to YES?)
        
        Args:
            time_buckets_days: Time horizons to analyze (days before market close)
            lookback_windows: Days to look back for recent trading activity metrics
        """
        print("\n" + "=" * 80)
        print("CREATING CALIBRATION DATASET WITH LIQUIDITY FEATURES")
        print("=" * 80)
        
        # Merge outcomes with trades
        df_with_outcomes = self.df.merge(
            self.market_outcomes[['market_id', 'last_trade_time', 'outcome', 'total_volume']],
            on='market_id',
            how='inner'
        )
        
        # Only keep markets with known outcomes
        df_with_outcomes = df_with_outcomes[df_with_outcomes['outcome'].notna()].copy()
        
        print(f"\nTrades with known outcomes: {len(df_with_outcomes):,}")
        
        # Calculate time remaining (in days)
        df_with_outcomes['time_remaining_days'] = (
            (df_with_outcomes['last_trade_time'] - df_with_outcomes['timestamp']) / 86400
        )
        
        # Filter to trades with positive time remaining
        df_with_outcomes = df_with_outcomes[df_with_outcomes['time_remaining_days'] > 0].copy()
        
        print("\nCalculating liquidity features...")
        
        # ====================================================================
        # LIQUIDITY FEATURE ENGINEERING
        # ====================================================================
        
        # 1. TOTAL MARKET VOLUME (already have this)
        df_with_outcomes['log_total_volume'] = np.log1p(df_with_outcomes['total_volume'])
        
        # 2. CUMULATIVE VOLUME UP TO THIS TRADE
        # This shows how much liquidity existed WHEN the trade occurred
        df_with_outcomes = df_with_outcomes.sort_values(['market_id', 'timestamp'])
        df_with_outcomes['cumulative_volume'] = df_with_outcomes.groupby('market_id')['usd_amount'].cumsum()
        df_with_outcomes['log_cumulative_volume'] = np.log1p(df_with_outcomes['cumulative_volume'])
        
        # 3. RECENT TRADING ACTIVITY (lookback windows)
        # Calculate volume in last N days before each trade
        print("  - Computing recent trading activity metrics...")
        
        for window in tqdm(lookback_windows, desc="Lookback windows"):
            window_seconds = window * 86400
            
            # For each trade, calculate volume in past N days
            recent_volumes = []
            recent_trade_counts = []
            
            for market_id in df_with_outcomes['market_id'].unique():
                market_trades = df_with_outcomes[df_with_outcomes['market_id'] == market_id].copy()
                market_trades = market_trades.sort_values('timestamp')
                
                for idx, row in market_trades.iterrows():
                    current_time = row['timestamp']
                    lookback_start = current_time - window_seconds
                    
                    # Get trades in the lookback window (excluding current trade)
                    recent = market_trades[
                        (market_trades['timestamp'] >= lookback_start) & 
                        (market_trades['timestamp'] < current_time)
                    ]
                    
                    recent_volume = recent['usd_amount'].sum()
                    recent_count = len(recent)
                    
                    recent_volumes.append(recent_volume)
                    recent_trade_counts.append(recent_count)
            
            df_with_outcomes[f'volume_last_{window}d'] = recent_volumes
            df_with_outcomes[f'trades_last_{window}d'] = recent_trade_counts
            df_with_outcomes[f'log_volume_last_{window}d'] = np.log1p(df_with_outcomes[f'volume_last_{window}d'])
            df_with_outcomes[f'trade_velocity_{window}d'] = df_with_outcomes[f'trades_last_{window}d'] / window
        
        # 4. MARKET MATURITY
        # How far into the market's lifecycle are we?
        df_with_outcomes['market_age_days'] = (
            (df_with_outcomes['timestamp'] - df_with_outcomes.groupby('market_id')['timestamp'].transform('min')) / 86400
        )
        df_with_outcomes['market_maturity'] = (
            df_with_outcomes['market_age_days'] / 
            (df_with_outcomes['market_age_days'] + df_with_outcomes['time_remaining_days'])
        )
        
        # 5. TRADE SIZE RELATIVE TO MARKET
        df_with_outcomes['trade_size_pct'] = (
            df_with_outcomes['usd_amount'] / df_with_outcomes['cumulative_volume']
        ).clip(0, 1)  # Clip to [0, 1] for first trades
        
        # 6. LIQUIDITY REGIME CLASSIFICATION
        # Classify markets into liquidity tiers based on total volume
        volume_percentiles = df_with_outcomes.groupby('market_id')['total_volume'].first().quantile([0.33, 0.67])
        
        def classify_liquidity(volume):
            if volume < volume_percentiles.iloc[0]:
                return 0  # Low liquidity
            elif volume < volume_percentiles.iloc[1]:
                return 1  # Medium liquidity
            else:
                return 2  # High liquidity
        
        df_with_outcomes['liquidity_regime'] = df_with_outcomes['total_volume'].apply(classify_liquidity)
        
        # 7. VOLUME CONCENTRATION (Gini coefficient approximation)
        # Measures how evenly distributed trading is over time
        print("  - Computing volume concentration metrics...")
        
        volume_concentration = []
        for market_id in tqdm(df_with_outcomes['market_id'].unique(), desc="Volume concentration"):
            market_trades = df_with_outcomes[df_with_outcomes['market_id'] == market_id]
            volumes = market_trades['usd_amount'].values
            
            # Simple concentration metric: std / mean
            if len(volumes) > 1 and volumes.sum() > 0:
                concentration = volumes.std() / (volumes.mean() + 1e-6)
            else:
                concentration = 0
            
            volume_concentration.extend([concentration] * len(market_trades))
        
        df_with_outcomes['volume_concentration'] = volume_concentration
        
        # ====================================================================
        # STANDARD FEATURES (as before)
        # ====================================================================
        
        df_with_outcomes['probability'] = df_with_outcomes['price']
        df_with_outcomes['log_time_remaining'] = np.log1p(df_with_outcomes['time_remaining_days'])
        df_with_outcomes['prob_squared'] = df_with_outcomes['probability'] ** 2
        df_with_outcomes['prob_cubed'] = df_with_outcomes['probability'] ** 3
        df_with_outcomes['prob_x_time'] = (df_with_outcomes['probability'] * 
                                            df_with_outcomes['log_time_remaining'])
        
        # INTERACTION FEATURES: Probability × Liquidity
        df_with_outcomes['prob_x_volume'] = (df_with_outcomes['probability'] * 
                                              df_with_outcomes['log_cumulative_volume'])
        df_with_outcomes['prob_x_liquidity_regime'] = (df_with_outcomes['probability'] * 
                                                        df_with_outcomes['liquidity_regime'])
        
        # Time × Liquidity interaction
        df_with_outcomes['time_x_volume'] = (df_with_outcomes['log_time_remaining'] * 
                                              df_with_outcomes['log_cumulative_volume'])
        
        # Probability bins for analysis
        df_with_outcomes['prob_bin'] = pd.cut(
            df_with_outcomes['probability'],
            bins=[0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
            labels=['0-10%', '10-20%', '20-30%', '30-40%', '40-50%', 
                    '50-60%', '60-70%', '70-80%', '80-90%', '90-100%']
        )
        
        # Time bins for analysis
        df_with_outcomes['time_bin'] = pd.cut(
            df_with_outcomes['time_remaining_days'],
            bins=[0] + time_buckets_days + [np.inf],
            labels=[f'0-{time_buckets_days[0]}d'] + 
                   [f'{time_buckets_days[i]}-{time_buckets_days[i+1]}d' 
                    for i in range(len(time_buckets_days)-1)] +
                   [f'{time_buckets_days[-1]}d+']
        )
        
        # Liquidity bins for analysis
        df_with_outcomes['volume_bin'] = pd.cut(
            df_with_outcomes['total_volume'],
            bins=[0, 1000, 10000, 50000, 100000, np.inf],
            labels=['<$1K', '$1K-10K', '$10K-50K', '$50K-100K', '$100K+']
        )
        
        self.calibration_data = df_with_outcomes
        
        print(f"\nFinal dataset size: {len(self.calibration_data):,} trades")
        print(f"Time remaining range: {self.calibration_data['time_remaining_days'].min():.1f} to "
              f"{self.calibration_data['time_remaining_days'].max():.1f} days")
        print(f"Volume range: ${self.calibration_data['total_volume'].min():,.0f} to "
              f"${self.calibration_data['total_volume'].max():,.0f}")
        
        # Liquidity statistics
        print("\nLiquidity Distribution:")
        print(self.calibration_data.groupby('liquidity_regime')['market_id'].nunique())
        print(f"  0 = Low liquidity ({self.calibration_data['total_volume'].quantile(0.33):,.0f})")
        print(f"  1 = Medium liquidity ({self.calibration_data['total_volume'].quantile(0.67):,.0f})")
        print(f"  2 = High liquidity")
        
        return self
        print(f"Time remaining range: {self.calibration_data['time_remaining_days'].min():.1f} to "
              f"{self.calibration_data['time_remaining_days'].max():.1f} days")
        
        return self
    
    def analyze_calibration(self, output_dir='calibration_analysis'):
        """
        Analyze raw calibration before modeling.
        Creates visualizations showing how well probabilities match outcomes,
        including liquidity-based analysis.
        """
        print("\n" + "=" * 80)
        print("ANALYZING CALIBRATION (with Liquidity Features)")
        print("=" * 80)
        
        Path(output_dir).mkdir(exist_ok=True)
        
        # Overall calibration curve
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        for idx, period in enumerate(['train', 'test']):
            data = self.calibration_data[self.calibration_data['period'] == period]
            
            # Calibration curve
            prob_true, prob_pred = calibration_curve(
                data['outcome'], data['probability'], n_bins=20, strategy='quantile'
            )
            
            ax = axes[idx, 0]
            ax.plot([0, 1], [0, 1], 'k--', label='Perfect calibration')
            ax.plot(prob_pred, prob_true, 'o-', linewidth=2, label='Polymarket')
            ax.set_xlabel('Predicted Probability')
            ax.set_ylabel('Actual Frequency')
            ax.set_title(f'Calibration Curve - {period.upper()}')
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            # Calibration by time remaining
            ax = axes[idx, 1]
            cal_by_time = data.groupby('time_bin').agg({
                'probability': 'mean',
                'outcome': 'mean',
                'market_id': 'count'
            }).reset_index()
            cal_by_time.columns = ['time_bin', 'avg_prob', 'actual_freq', 'count']
            
            x = range(len(cal_by_time))
            ax.bar(x, cal_by_time['avg_prob'], alpha=0.5, label='Avg Probability')
            ax.bar(x, cal_by_time['actual_freq'], alpha=0.5, label='Actual Frequency')
            ax.set_xticks(x)
            ax.set_xticklabels(cal_by_time['time_bin'], rotation=45, ha='right')
            ax.set_ylabel('Probability')
            ax.set_title(f'Calibration by Time Remaining - {period.upper()}')
            ax.legend()
            ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f'{output_dir}/calibration_overview.png', dpi=300, bbox_inches='tight')
        print(f"Saved: {output_dir}/calibration_overview.png")
        plt.close()
        
        # Detailed calibration heatmap (Probability × Time)
        fig, axes = plt.subplots(1, 2, figsize=(18, 6))
        
        for idx, period in enumerate(['train', 'test']):
            data = self.calibration_data[self.calibration_data['period'] == period]
            
            pivot = data.groupby(['prob_bin', 'time_bin'])['outcome'].mean().unstack()
            
            sns.heatmap(pivot, annot=True, fmt='.2f', cmap='RdYlGn', 
                       center=0.5, vmin=0, vmax=1, ax=axes[idx],
                       cbar_kws={'label': 'Actual Outcome Rate'})
            axes[idx].set_title(f'Calibration Heatmap - {period.upper()}')
            axes[idx].set_xlabel('Time Remaining')
            axes[idx].set_ylabel('Displayed Probability')
        
        plt.tight_layout()
        plt.savefig(f'{output_dir}/calibration_heatmap.png', dpi=300, bbox_inches='tight')
        print(f"Saved: {output_dir}/calibration_heatmap.png")
        plt.close()
        
        # NEW: Calibration by Liquidity Regime
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        for idx, period in enumerate(['train', 'test']):
            data = self.calibration_data[self.calibration_data['period'] == period]
            
            # Calibration curves by liquidity regime
            ax = axes[idx, 0]
            ax.plot([0, 1], [0, 1], 'k--', alpha=0.5, label='Perfect')
            
            colors = ['red', 'orange', 'green']
            labels = ['Low Liquidity', 'Medium Liquidity', 'High Liquidity']
            
            for liq_regime in [0, 1, 2]:
                liq_data = data[data['liquidity_regime'] == liq_regime]
                if len(liq_data) > 0:
                    prob_true, prob_pred = calibration_curve(
                        liq_data['outcome'], liq_data['probability'], 
                        n_bins=10, strategy='quantile'
                    )
                    ax.plot(prob_pred, prob_true, 'o-', linewidth=2, 
                           color=colors[liq_regime], label=labels[liq_regime])
            
            ax.set_xlabel('Predicted Probability')
            ax.set_ylabel('Actual Frequency')
            ax.set_title(f'Calibration by Liquidity - {period.upper()}')
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            # Calibration error by volume bin
            ax = axes[idx, 1]
            cal_by_volume = data.groupby('volume_bin').agg({
                'probability': 'mean',
                'outcome': 'mean',
                'market_id': 'nunique'
            }).reset_index()
            cal_by_volume.columns = ['volume_bin', 'avg_prob', 'actual_freq', 'n_markets']
            cal_by_volume['error'] = abs(cal_by_volume['avg_prob'] - cal_by_volume['actual_freq'])
            
            x = range(len(cal_by_volume))
            bars = ax.bar(x, cal_by_volume['error'], color='steelblue')
            ax.set_xticks(x)
            ax.set_xticklabels(cal_by_volume['volume_bin'], rotation=45, ha='right')
            ax.set_ylabel('Absolute Calibration Error')
            ax.set_title(f'Calibration Error by Volume - {period.upper()}')
            ax.grid(True, alpha=0.3, axis='y')
            
            # Add market count labels
            for i, (bar, count) in enumerate(zip(bars, cal_by_volume['n_markets'])):
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'n={count}', ha='center', va='bottom', fontsize=8)
        
        plt.tight_layout()
        plt.savefig(f'{output_dir}/calibration_by_liquidity.png', dpi=300, bbox_inches='tight')
        print(f"Saved: {output_dir}/calibration_by_liquidity.png")
        plt.close()
        
        # NEW: 3D Heatmap (Probability × Time × Liquidity)
        fig, axes = plt.subplots(1, 3, figsize=(20, 6))
        
        period = 'train'  # Focus on training data for this viz
        data = self.calibration_data[self.calibration_data['period'] == period]
        
        for liq_idx, liq_regime in enumerate([0, 1, 2]):
            liq_data = data[data['liquidity_regime'] == liq_regime]
            
            if len(liq_data) > 0:
                pivot = liq_data.groupby(['prob_bin', 'time_bin'])['outcome'].mean().unstack()
                
                sns.heatmap(pivot, annot=True, fmt='.2f', cmap='RdYlGn', 
                           center=0.5, vmin=0, vmax=1, ax=axes[liq_idx],
                           cbar_kws={'label': 'Actual Outcome Rate'})
                axes[liq_idx].set_title(f'{labels[liq_regime]} Markets')
                axes[liq_idx].set_xlabel('Time Remaining')
                axes[liq_idx].set_ylabel('Displayed Probability')
        
        plt.tight_layout()
        plt.savefig(f'{output_dir}/calibration_3d_heatmap.png', dpi=300, bbox_inches='tight')
        print(f"Saved: {output_dir}/calibration_3d_heatmap.png")
        plt.close()
        
        return self
    
    def train_model(self, features=None):
        """
        Train XGBoost model to predict true probability from displayed probability,
        time remaining, and liquidity features.
        
        Args:
            features: List of feature column names to use. If None, uses enhanced default set.
        """
        print("\n" + "=" * 80)
        print("TRAINING CALIBRATION MODEL (with Liquidity Features)")
        print("=" * 80)
        
        if features is None:
            # Enhanced feature set with comprehensive liquidity metrics
            features = [
                # Core features
                'probability',           # Main feature: displayed probability
                'log_time_remaining',    # Log of days remaining
                
                # Probability polynomial features
                'prob_squared',          # Non-linear probability effects
                'prob_cubed',
                
                # Liquidity features
                'log_cumulative_volume',      # Volume accumulated so far
                'log_volume_last_7d',         # Recent trading activity (7 days)
                'log_volume_last_30d',        # Longer-term activity (30 days)
                'trade_velocity_7d',          # Trades per day (7d window)
                'liquidity_regime',           # Low/Med/High classification
                'volume_concentration',       # How concentrated is trading
                'market_maturity',            # How far into market lifecycle
                
                # Interaction features
                'prob_x_time',           # Probability × time
                'prob_x_volume',         # Probability × liquidity
                'time_x_volume',         # Time × liquidity
                'prob_x_liquidity_regime',  # Probability × liquidity tier
            ]
        
        # Prepare training data
        train_data = self.calibration_data[self.calibration_data['period'] == 'train'].copy()
        
        # Check for missing features
        missing_features = [f for f in features if f not in train_data.columns]
        if missing_features:
            print(f"\nWarning: Missing features: {missing_features}")
            print("Using available features only.")
            features = [f for f in features if f in train_data.columns]
        
        X_train = train_data[features]
        y_train = train_data['outcome']
        
        print(f"\nTraining samples: {len(X_train):,}")
        print(f"Number of features: {len(features)}")
        print(f"\nFeatures used:")
        for i, feat in enumerate(features, 1):
            print(f"  {i:2d}. {feat}")
        
        # XGBoost model with enhanced parameters for complex interactions
        print("\nTraining XGBoost model...")
        self.model = xgb.XGBClassifier(
            n_estimators=500,
            max_depth=7,              # Increased to capture liquidity interactions
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=5,       # Regularization for robustness
            gamma=0.1,                # Minimum loss reduction
            objective='binary:logistic',
            eval_metric='logloss',
            random_state=42,
            n_jobs=-1
        )
        
        # Train with early stopping
        X_tr, X_val, y_tr, y_val = train_test_split(
            X_train, y_train, test_size=0.2, random_state=42
        )
        
        self.model.fit(
            X_tr, y_tr,
            eval_set=[(X_val, y_val)],
            verbose=50
        )
        
        # Feature importance analysis
        importance_df = pd.DataFrame({
            'feature': features,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        print("\n" + "=" * 60)
        print("Feature Importance Rankings:")
        print("=" * 60)
        for idx, row in importance_df.iterrows():
            print(f"{row['feature']:30s} | {row['importance']:.4f}")
        
        # Identify top liquidity features
        liquidity_features = [f for f in features if any(x in f.lower() for x in 
                             ['volume', 'liquidity', 'velocity', 'concentration', 'maturity'])]
        liquidity_importance = importance_df[importance_df['feature'].isin(liquidity_features)]
        
        if len(liquidity_importance) > 0:
            total_liquidity_importance = liquidity_importance['importance'].sum()
            print("\n" + "=" * 60)
            print(f"Total Liquidity Feature Importance: {total_liquidity_importance:.1%}")
            print("=" * 60)
            print("\nTop Liquidity Features:")
            for idx, row in liquidity_importance.head(5).iterrows():
                print(f"  {row['feature']:30s} | {row['importance']:.4f}")
        
        # Plot feature importance
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))
        
        # All features
        top_n = min(15, len(importance_df))
        top_features = importance_df.head(top_n)
        ax1.barh(range(top_n), top_features['importance'].values)
        ax1.set_yticks(range(top_n))
        ax1.set_yticklabels(top_features['feature'].values)
        ax1.set_xlabel('Importance')
        ax1.set_title(f'Top {top_n} Feature Importance')
        ax1.invert_yaxis()
        ax1.grid(True, alpha=0.3, axis='x')
        
        # Feature importance by category
        categories = {
            'Probability': ['probability', 'prob_squared', 'prob_cubed'],
            'Time': ['log_time_remaining'],
            'Liquidity': liquidity_features,
            'Interactions': [f for f in features if '_x_' in f]
        }
        
        category_importance = {}
        for cat, feats in categories.items():
            cat_feats = [f for f in feats if f in features]
            if cat_feats:
                cat_imp = importance_df[importance_df['feature'].isin(cat_feats)]['importance'].sum()
                category_importance[cat] = cat_imp
        
        if category_importance:
            cats = list(category_importance.keys())
            imps = list(category_importance.values())
            colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A']
            
            ax2.barh(cats, imps, color=colors[:len(cats)])
            ax2.set_xlabel('Total Importance')
            ax2.set_title('Feature Importance by Category')
            ax2.grid(True, alpha=0.3, axis='x')
            
            # Add percentage labels
            for i, (cat, imp) in enumerate(zip(cats, imps)):
                ax2.text(imp, i, f' {imp:.1%}', va='center')
        
        plt.tight_layout()
        plt.savefig('calibration_analysis/feature_importance_liquidity.png', dpi=300, bbox_inches='tight')
        print(f"\nSaved: calibration_analysis/feature_importance_liquidity.png")
        plt.close()
        
        self.features = features
        return self
    
    def evaluate_model(self, output_dir='calibration_analysis'):
        """
        Evaluate the trained model on both training and test sets.
        """
        print("\n" + "=" * 80)
        print("EVALUATING MODEL")
        print("=" * 80)
        
        Path(output_dir).mkdir(exist_ok=True)
        
        results = {}
        
        for period in ['train', 'test']:
            data = self.calibration_data[self.calibration_data['period'] == period].copy()
            
            X = data[self.features]
            y_true = data['outcome']
            
            # Predictions
            y_pred_proba = self.model.predict_proba(X)[:, 1]
            data['calibrated_prob'] = y_pred_proba
            
            # Metrics
            mse = mean_squared_error(y_true, y_pred_proba)
            mae = mean_absolute_error(y_true, y_pred_proba)
            logloss = log_loss(y_true, y_pred_proba)
            
            # Brier score (MSE for probability predictions)
            brier_raw = mean_squared_error(y_true, data['probability'])
            brier_calibrated = mse
            
            results[period] = {
                'mse': mse,
                'mae': mae,
                'logloss': logloss,
                'brier_raw': brier_raw,
                'brier_calibrated': brier_calibrated,
                'brier_improvement': (brier_raw - brier_calibrated) / brier_raw * 100
            }
            
            print(f"\n{period.upper()} SET METRICS:")
            print(f"  Raw Brier Score:        {brier_raw:.6f}")
            print(f"  Calibrated Brier Score: {brier_calibrated:.6f}")
            print(f"  Improvement:            {results[period]['brier_improvement']:.2f}%")
            print(f"  Log Loss:               {logloss:.6f}")
            print(f"  MAE:                    {mae:.6f}")
        
        # Visualization: Before vs After Calibration
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        for idx, period in enumerate(['train', 'test']):
            data = self.calibration_data[self.calibration_data['period'] == period].copy()
            data['calibrated_prob'] = self.model.predict_proba(data[self.features])[:, 1]
            
            # Before calibration
            ax = axes[idx, 0]
            prob_true, prob_pred = calibration_curve(
                data['outcome'], data['probability'], n_bins=20, strategy='quantile'
            )
            ax.plot([0, 1], [0, 1], 'k--', alpha=0.5, label='Perfect')
            ax.plot(prob_pred, prob_true, 'o-', linewidth=2, label='Raw Polymarket', color='red')
            ax.set_xlabel('Predicted Probability')
            ax.set_ylabel('Actual Frequency')
            ax.set_title(f'Before Calibration - {period.upper()}')
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            # After calibration
            ax = axes[idx, 1]
            prob_true_cal, prob_pred_cal = calibration_curve(
                data['outcome'], data['calibrated_prob'], n_bins=20, strategy='quantile'
            )
            ax.plot([0, 1], [0, 1], 'k--', alpha=0.5, label='Perfect')
            ax.plot(prob_pred, prob_true, 'o-', linewidth=1, alpha=0.5, 
                   label='Raw', color='red')
            ax.plot(prob_pred_cal, prob_true_cal, 'o-', linewidth=2, 
                   label='Calibrated', color='green')
            ax.set_xlabel('Predicted Probability')
            ax.set_ylabel('Actual Frequency')
            ax.set_title(f'After Calibration - {period.upper()}')
            ax.legend()
            ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f'{output_dir}/calibration_comparison.png', dpi=300, bbox_inches='tight')
        print(f"\nSaved: {output_dir}/calibration_comparison.png")
        plt.close()
        
        return results
    
    def predict_calibrated_probability(self, displayed_prob: float, 
                                       days_remaining: float,
                                       cumulative_volume: float = 50000,
                                       volume_last_7d: float = 10000,
                                       volume_last_30d: float = 40000,
                                       market_age_days: float = 30,
                                       trades_last_7d: float = 20) -> float:
        """
        Predict the calibrated (true) probability given market conditions.
        
        Args:
            displayed_prob: The probability shown on Polymarket (0-1)
            days_remaining: Days until market resolution
            cumulative_volume: Total volume traded so far in USD (default: 50k)
            volume_last_7d: Volume in last 7 days in USD (default: 10k)
            volume_last_30d: Volume in last 30 days in USD (default: 40k)
            market_age_days: How many days the market has been active (default: 30)
            trades_last_7d: Number of trades in last 7 days (default: 20)
        
        Returns:
            Calibrated probability (0-1)
        """
        # Calculate derived features
        log_time = np.log1p(days_remaining)
        log_cumulative_vol = np.log1p(cumulative_volume)
        log_vol_7d = np.log1p(volume_last_7d)
        log_vol_30d = np.log1p(volume_last_30d)
        trade_velocity = trades_last_7d / 7
        market_maturity = market_age_days / (market_age_days + days_remaining)
        
        # Classify liquidity regime (using rough heuristics)
        if cumulative_volume < 10000:
            liquidity_regime = 0  # Low
        elif cumulative_volume < 100000:
            liquidity_regime = 1  # Medium
        else:
            liquidity_regime = 2  # High
        
        # Estimate volume concentration (simple heuristic: more recent = more concentrated)
        volume_concentration = volume_last_7d / (volume_last_30d + 1) if volume_last_30d > 0 else 0.5
        
        # Prepare features (must match training features)
        features_dict = {
            'probability': displayed_prob,
            'log_time_remaining': log_time,
            'prob_squared': displayed_prob ** 2,
            'prob_cubed': displayed_prob ** 3,
            'log_cumulative_volume': log_cumulative_vol,
            'log_volume_last_7d': log_vol_7d,
            'log_volume_last_30d': log_vol_30d,
            'trade_velocity_7d': trade_velocity,
            'liquidity_regime': liquidity_regime,
            'volume_concentration': volume_concentration,
            'market_maturity': market_maturity,
            'prob_x_time': displayed_prob * log_time,
            'prob_x_volume': displayed_prob * log_cumulative_vol,
            'time_x_volume': log_time * log_cumulative_vol,
            'prob_x_liquidity_regime': displayed_prob * liquidity_regime,
        }
        
        # Filter to only features used in model
        available_features = {k: v for k, v in features_dict.items() if k in self.features}
        
        X = pd.DataFrame([available_features])[self.features]
        
        calibrated_prob = self.model.predict_proba(X)[0, 1]
        
        return calibrated_prob
            'log_volume': np.log1p(market_volume),
        }
        
        # Filter to only features used in model
        available_features = {k: v for k, v in features_dict.items() if k in self.features}
        
        X = pd.DataFrame([available_features])[self.features]
        
        calibrated_prob = self.model.predict_proba(X)[0, 1]
        
        return calibrated_prob
    
    def save_model(self, filepath='polymarket_calibration_model.json'):
        """Save the trained model."""
        self.model.save_model(filepath)
        print(f"\nModel saved to: {filepath}")
    
    def load_model(self, filepath='polymarket_calibration_model.json'):
        """Load a previously trained model."""
        self.model = xgb.XGBClassifier()
        self.model.load_model(filepath)
        print(f"\nModel loaded from: {filepath}")


def main():
    """
    Main execution function with example usage.
    """
    print("\n" + "=" * 80)
    print("POLYMARKET PROBABILITY CALIBRATION ANALYSIS")
    print("=" * 80)
    
    # ========================================================================
    # CONFIGURATION - MODIFY THESE PARAMETERS
    # ========================================================================
    
    PARQUET_PATH = r"C:\Users\2same\Economics BSc\Quant\PolyQuant\data_probability\quant.parquet"
    
    # Training period
    TRAIN_START = "2023-01-01"
    TRAIN_END = "2023-12-31"
    
    # Testing period
    TEST_START = "2024-01-01"
    TEST_END = "2024-06-30"
    
    # ========================================================================
    
    # Initialize analyzer
    analyzer = PolymarketCalibration(PARQUET_PATH)
    
    # Load data
    analyzer.load_data(
        train_start=TRAIN_START,
        train_end=TRAIN_END,
        test_start=TEST_START,
        test_end=TEST_END,
        chunk_size=1_000_000  # Adjust based on your RAM
    )
    
    # Get market outcomes
    analyzer.get_market_outcomes(polymarket_api_or_heuristic='heuristic')
    
    # Create calibration dataset
    analyzer.create_calibration_dataset(
        time_buckets_days=[1, 3, 7, 14, 30, 60, 90, 180]
    )
    
    # Analyze raw calibration
    analyzer.analyze_calibration()
    
    # Train model
    analyzer.train_model()
    
    # Evaluate model
    results = analyzer.evaluate_model()
    
    # Save model
    analyzer.save_model('polymarket_calibration_model.json')
    
    # ========================================================================
    # EXAMPLE PREDICTIONS
    # ========================================================================
    
    print("\n" + "=" * 80)
    print("EXAMPLE CALIBRATED PREDICTIONS")
    print("=" * 80)
    
    examples = [
        (0.70, 30, 100000),   # 70% with 30 days remaining
        (0.70, 7, 100000),    # 70% with 7 days remaining
        (0.70, 1, 100000),    # 70% with 1 day remaining
        (0.90, 30, 500000),   # 90% with 30 days remaining
        (0.50, 60, 50000),    # 50% with 60 days remaining
    ]
    
    print("\nDisplayed% | Days Left | Volume    | Calibrated%")
    print("-" * 55)
    for disp_prob, days, volume in examples:
        cal_prob = analyzer.predict_calibrated_probability(disp_prob, days, volume)
        diff = cal_prob - disp_prob
        sign = "+" if diff > 0 else ""
        print(f"   {disp_prob*100:4.0f}%   |    {days:3.0f}    | ${volume:7,.0f} | "
              f"{cal_prob*100:5.1f}% ({sign}{diff*100:.1f}%)")
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE!")
    print("=" * 80)
    print("\nGenerated files:")
    print("  - calibration_analysis/calibration_overview.png")
    print("  - calibration_analysis/calibration_heatmap.png")
    print("  - calibration_analysis/feature_importance.png")
    print("  - calibration_analysis/calibration_comparison.png")
    print("  - polymarket_calibration_model.json")
    print("\n")


if __name__ == "__main__":
    main()
