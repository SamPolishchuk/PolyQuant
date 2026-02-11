# Polymarket Probability Calibration Model

## Overview

This tool analyzes the **calibration** of Polymarket probabilities over time. Calibration measures how well displayed probabilities match actual outcomes. For example:
- If Polymarket shows 70%, do events actually occur 70% of the time?
- Does calibration change as you get closer to the event resolution date?

## What This Does

1. **Loads your 30GB parquet file** in efficient chunks
2. **Determines market outcomes** (YES/NO resolution)
3. **Creates calibration dataset** linking displayed probabilities → actual outcomes
4. **Trains an XGBoost model** to predict true probabilities
5. **Evaluates performance** on separate test period
6. **Generates visualizations** showing calibration improvements

## Model Architecture

### Algorithm: XGBoost Gradient Boosting

**Why XGBoost?**
- Handles non-linear relationships between time-to-resolution and calibration
- Captures complex interactions (e.g., how calibration at 70% differs from 30%)
- Robust to different market dynamics
- Fast training even on large datasets
- Built-in regularization prevents overfitting

### Features Used

The model uses these features to predict true probability:

1. **probability** - The displayed Polymarket probability (0-1)
2. **log_time_remaining** - Log of days until resolution
3. **prob_squared** - Probability squared (captures non-linearity)
4. **prob_cubed** - Probability cubed (extreme probability behavior)
5. **prob_x_time** - Interaction: probability × time remaining
6. **log_volume** - Market liquidity (more volume = more reliable)

### Target Variable

**outcome** - Binary (0/1) indicating whether the market resolved to YES

## Installation

```bash
pip install pandas pyarrow xgboost scikit-learn matplotlib seaborn tqdm
```

## Usage

### Basic Usage

```python
python polymarket_calibration.py
```

### Customization

Edit these parameters in the `main()` function:

```python
# Your file path
PARQUET_PATH = r"C:\Users\2same\Economics BSc\Quant\PolyQuant\data_probability\quant.parquet"

# Training period (MODIFY THESE)
TRAIN_START = "2023-01-01"
TRAIN_END = "2023-12-31"

# Testing period (MODIFY THESE)
TEST_START = "2024-01-01"
TEST_END = "2024-06-30"
```

**Important Notes:**
- Training and testing periods should NOT overlap
- Larger training periods = better model, but slower
- Test period should be recent to evaluate real-world performance

### Advanced Usage

```python
from polymarket_calibration import PolymarketCalibration

# Initialize
analyzer = PolymarketCalibration("path/to/quant.parquet")

# Load data
analyzer.load_data(
    train_start="2023-01-01",
    train_end="2023-12-31",
    test_start="2024-01-01",
    test_end="2024-06-30",
    chunk_size=2_000_000  # Increase if you have lots of RAM
)

# Process
analyzer.get_market_outcomes()
analyzer.create_calibration_dataset()
analyzer.analyze_calibration()
analyzer.train_model()
results = analyzer.evaluate_model()

# Make predictions
calibrated_prob = analyzer.predict_calibrated_probability(
    displayed_prob=0.70,  # Polymarket shows 70%
    days_remaining=30,    # 30 days until resolution
    market_volume=100000  # $100k total volume
)

print(f"Calibrated probability: {calibrated_prob:.1%}")
```

## How Market Outcomes Are Determined

The script uses a **heuristic approach**:
- Takes the last 100 trades for each market
- Calculates average final price
- If final price > 0.9 → Outcome = YES
- If final price < 0.1 → Outcome = NO
- Otherwise → UNKNOWN (excluded from analysis)

**Note:** For production use, you may want to:
1. Use Polymarket's API to get official resolutions
2. Adjust thresholds (0.9/0.1) based on your data
3. Handle edge cases (cancelled markets, etc.)

## Output Files

The script generates:

1. **calibration_overview.png** - Overall calibration curves (train/test)
2. **calibration_heatmap.png** - Calibration by probability × time matrix
3. **feature_importance.png** - Which features matter most
4. **calibration_comparison.png** - Before/after calibration
5. **polymarket_calibration_model.json** - Trained model (reusable)

## Interpreting Results

### Brier Score

The main metric is **Brier Score** (lower is better):
- Measures accuracy of probability predictions
- Raw Brier = uncalibrated Polymarket probabilities
- Calibrated Brier = model-adjusted probabilities
- **Improvement %** = how much better the model is

Example output:
```
TRAIN SET METRICS:
  Raw Brier Score:        0.184523
  Calibrated Brier Score: 0.176891
  Improvement:            4.14%
```

### Calibration Curves

**Perfect calibration** = diagonal line
- Points above the line = overconfident (70% actually happens 60% of the time)
- Points below the line = underconfident (70% actually happens 80% of the time)

### Example Predictions

```
Displayed% | Days Left | Volume    | Calibrated%
-------------------------------------------------------
   70%     |     30    | $100,000  |  67.3% (-2.7%)
   70%     |      7    | $100,000  |  69.8% (-0.2%)
   70%     |      1    | $100,000  |  70.5% (+0.5%)
   90%     |     30    | $500,000  |  88.1% (-1.9%)
   50%     |     60    | $ 50,000  |  51.2% (+1.2%)
```

**Interpretation:**
- 70% probability with 30 days left is actually ~67% (slightly overconfident)
- 70% probability with 1 day left is ~70.5% (well calibrated)
- Higher volume markets tend to be better calibrated

## Memory Considerations

For a **30GB parquet file**:
- Default `chunk_size=1_000_000` works with 16GB RAM
- Increase to `2_000_000` if you have 32GB+ RAM
- Decrease to `500_000` if you have 8GB RAM

The script processes data in chunks and only keeps relevant columns in memory.

## Extending the Model

### Add More Features

```python
# In create_calibration_dataset():
df_with_outcomes['num_trades'] = df_with_outcomes.groupby('market_id')['market_id'].transform('count')
df_with_outcomes['price_volatility'] = df_with_outcomes.groupby('market_id')['price'].transform('std')

# In train_model():
features = [
    'probability',
    'log_time_remaining',
    'prob_squared',
    'prob_cubed',
    'prob_x_time',
    'log_volume',
    'num_trades',        # NEW
    'price_volatility',  # NEW
]
```

### Try Different Models

```python
# Random Forest
from sklearn.ensemble import RandomForestClassifier

self.model = RandomForestClassifier(
    n_estimators=200,
    max_depth=10,
    random_state=42
)

# LightGBM (faster than XGBoost)
import lightgbm as lgb

self.model = lgb.LGBMClassifier(
    n_estimators=500,
    max_depth=6,
    learning_rate=0.05
)
```

### Filter by Market Type

```python
# Only analyze high-volume markets
df_with_outcomes = df_with_outcomes[
    df_with_outcomes['total_volume'] > 10000
]

# Only analyze specific event types
df_with_outcomes = df_with_outcomes[
    df_with_outcomes['question'].str.contains('election|politics', case=False)
]
```

## Troubleshooting

### "No data found in the specified date ranges"
- Check your date formats (YYYY-MM-DD)
- Verify your parquet file contains data in those periods
- Try broader date ranges

### Out of Memory
- Reduce `chunk_size` parameter
- Process fewer markets at once
- Use a machine with more RAM

### Poor Model Performance
- Increase training period (more data = better model)
- Check if test period is too different from training period
- Verify market outcomes are being detected correctly

### Slow Processing
- Increase `chunk_size` (faster but uses more RAM)
- Use fewer time buckets
- Filter to high-volume markets only

## Research Questions You Can Answer

1. **Does calibration improve closer to resolution?**
   - Compare model predictions at 30 days vs 1 day

2. **Are certain probability ranges poorly calibrated?**
   - Check the calibration heatmap

3. **Does market volume affect calibration?**
   - Examine feature importance of `log_volume`

4. **Has Polymarket's calibration improved over time?**
   - Train separate models for different time periods

5. **Which market types are best/worst calibrated?**
   - Filter by question keywords and compare

## Citation

If you use this for research:
```
Polymarket Calibration Model
XGBoost-based probability calibration for prediction markets
Features: displayed probability, time remaining, market volume
Training: [Your dates]
```

## License

MIT License - feel free to modify and extend!

## Contact

For questions about the methodology or implementation, refer to:
- XGBoost documentation: https://xgboost.readthedocs.io/
- Calibration theory: https://en.wikipedia.org/wiki/Calibration_(statistics)
