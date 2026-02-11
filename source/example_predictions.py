"""
Example: Using the Trained Calibration Model
=============================================

This script shows how to use a trained calibration model to make predictions
on new Polymarket data.
"""

import pandas as pd
import numpy as np
import xgboost as xgb


def predict_calibrated_probability(model_path: str,
                                   displayed_prob: float,
                                   days_remaining: float,
                                   market_volume: float = 100000) -> dict:
    """
    Predict the calibrated (true) probability for a Polymarket market.
    
    Args:
        model_path: Path to trained model JSON file
        displayed_prob: The probability shown on Polymarket (0-1)
        days_remaining: Days until market resolution
        market_volume: Total market volume in USD
    
    Returns:
        Dictionary with prediction details
    """
    # Load model
    model = xgb.XGBClassifier()
    model.load_model(model_path)
    
    # Prepare features (must match training)
    features_dict = {
        'probability': displayed_prob,
        'log_time_remaining': np.log1p(days_remaining),
        'prob_squared': displayed_prob ** 2,
        'prob_cubed': displayed_prob ** 3,
        'prob_x_time': displayed_prob * np.log1p(days_remaining),
        'log_volume': np.log1p(market_volume),
    }
    
    feature_names = [
        'probability',
        'log_time_remaining',
        'prob_squared',
        'prob_cubed',
        'prob_x_time',
        'log_volume',
    ]
    
    X = pd.DataFrame([features_dict])[feature_names]
    
    # Predict
    calibrated_prob = model.predict_proba(X)[0, 1]
    
    # Calculate adjustment
    adjustment = calibrated_prob - displayed_prob
    adjustment_pct = (adjustment / displayed_prob * 100) if displayed_prob > 0 else 0
    
    return {
        'displayed_probability': displayed_prob,
        'calibrated_probability': calibrated_prob,
        'adjustment': adjustment,
        'adjustment_percent': adjustment_pct,
        'days_remaining': days_remaining,
        'market_volume': market_volume,
    }


def batch_predict(model_path: str, markets: list) -> pd.DataFrame:
    """
    Make predictions for multiple markets at once.
    
    Args:
        model_path: Path to trained model
        markets: List of dicts with keys: 'displayed_prob', 'days_remaining', 'market_volume'
    
    Returns:
        DataFrame with predictions
    """
    results = []
    
    for market in markets:
        pred = predict_calibrated_probability(
            model_path=model_path,
            displayed_prob=market['displayed_prob'],
            days_remaining=market['days_remaining'],
            market_volume=market.get('market_volume', 100000)
        )
        
        # Add market identifier if provided
        if 'market_id' in market:
            pred['market_id'] = market['market_id']
        if 'question' in market:
            pred['question'] = market['question']
        
        results.append(pred)
    
    return pd.DataFrame(results)


def analyze_calibration_adjustments(model_path: str):
    """
    Analyze how calibration adjustments vary across probability ranges and time horizons.
    """
    import matplotlib.pyplot as plt
    import seaborn as sns
    
    # Create grid of probabilities and time horizons
    probabilities = np.linspace(0.05, 0.95, 19)  # 5% to 95% in 5% steps
    time_horizons = [1, 3, 7, 14, 30, 60, 90, 180]  # days
    
    results = []
    
    for prob in probabilities:
        for days in time_horizons:
            pred = predict_calibrated_probability(
                model_path=model_path,
                displayed_prob=prob,
                days_remaining=days,
                market_volume=100000
            )
            results.append({
                'probability': prob,
                'days_remaining': days,
                'calibrated_prob': pred['calibrated_probability'],
                'adjustment': pred['adjustment']
            })
    
    df = pd.DataFrame(results)
    
    # Plot heatmap of adjustments
    pivot = df.pivot_table(
        values='adjustment',
        index='probability',
        columns='days_remaining'
    )
    
    plt.figure(figsize=(12, 8))
    sns.heatmap(
        pivot * 100,  # Convert to percentage points
        annot=True,
        fmt='.1f',
        cmap='RdBu_r',
        center=0,
        cbar_kws={'label': 'Adjustment (percentage points)'}
    )
    plt.xlabel('Days Remaining')
    plt.ylabel('Displayed Probability')
    plt.title('Calibration Adjustments by Probability and Time Horizon')
    plt.tight_layout()
    plt.savefig('calibration_adjustments_heatmap.png', dpi=300, bbox_inches='tight')
    print("Saved: calibration_adjustments_heatmap.png")
    plt.close()
    
    return df


def find_mispriced_markets(model_path: str, current_markets: pd.DataFrame,
                          threshold: float = 0.05) -> pd.DataFrame:
    """
    Identify markets where calibrated probability differs significantly from displayed.
    
    Args:
        model_path: Path to trained model
        current_markets: DataFrame with columns: market_id, displayed_prob, days_remaining, market_volume
        threshold: Minimum absolute difference to flag (default: 5 percentage points)
    
    Returns:
        DataFrame of potentially mispriced markets
    """
    predictions = []
    
    for _, market in current_markets.iterrows():
        pred = predict_calibrated_probability(
            model_path=model_path,
            displayed_prob=market['displayed_prob'],
            days_remaining=market['days_remaining'],
            market_volume=market.get('market_volume', 100000)
        )
        
        pred['market_id'] = market['market_id']
        if 'question' in market:
            pred['question'] = market['question']
        
        predictions.append(pred)
    
    df = pd.DataFrame(predictions)
    
    # Filter to significant mispricing
    df['abs_adjustment'] = abs(df['adjustment'])
    mispriced = df[df['abs_adjustment'] >= threshold].copy()
    mispriced = mispriced.sort_values('abs_adjustment', ascending=False)
    
    return mispriced


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    MODEL_PATH = "polymarket_calibration_model.json"
    
    print("=" * 80)
    print("POLYMARKET CALIBRATION PREDICTIONS")
    print("=" * 80)
    
    # Example 1: Single prediction
    print("\n1. Single Market Prediction")
    print("-" * 80)
    
    result = predict_calibrated_probability(
        model_path=MODEL_PATH,
        displayed_prob=0.70,
        days_remaining=30,
        market_volume=250000
    )
    
    print(f"Displayed Probability:   {result['displayed_probability']:.1%}")
    print(f"Calibrated Probability:  {result['calibrated_probability']:.1%}")
    print(f"Adjustment:              {result['adjustment']:+.1%} ({result['adjustment_percent']:+.1f}%)")
    print(f"Days Remaining:          {result['days_remaining']:.0f}")
    print(f"Market Volume:           ${result['market_volume']:,.0f}")
    
    # Example 2: Batch predictions
    print("\n2. Batch Predictions")
    print("-" * 80)
    
    markets = [
        {'market_id': 'A', 'question': 'Will X happen?', 'displayed_prob': 0.65, 'days_remaining': 45, 'market_volume': 150000},
        {'market_id': 'B', 'question': 'Will Y happen?', 'displayed_prob': 0.80, 'days_remaining': 10, 'market_volume': 500000},
        {'market_id': 'C', 'question': 'Will Z happen?', 'displayed_prob': 0.50, 'days_remaining': 90, 'market_volume': 75000},
        {'market_id': 'D', 'question': 'Will W happen?', 'displayed_prob': 0.25, 'days_remaining': 5, 'market_volume': 200000},
    ]
    
    results_df = batch_predict(MODEL_PATH, markets)
    
    print(results_df[['market_id', 'question', 'displayed_probability', 
                      'calibrated_probability', 'adjustment']].to_string(index=False))
    
    # Example 3: Find mispriced markets
    print("\n3. Finding Mispriced Markets (>5pp adjustment)")
    print("-" * 80)
    
    current_markets = pd.DataFrame([
        {'market_id': 'M1', 'question': 'Election outcome', 'displayed_prob': 0.75, 'days_remaining': 60, 'market_volume': 1000000},
        {'market_id': 'M2', 'question': 'Sports game', 'displayed_prob': 0.55, 'days_remaining': 2, 'market_volume': 50000},
        {'market_id': 'M3', 'question': 'Economic indicator', 'displayed_prob': 0.40, 'days_remaining': 120, 'market_volume': 80000},
        {'market_id': 'M4', 'question': 'Tech announcement', 'displayed_prob': 0.90, 'days_remaining': 15, 'market_volume': 300000},
    ])
    
    mispriced = find_mispriced_markets(MODEL_PATH, current_markets, threshold=0.03)
    
    if len(mispriced) > 0:
        print(mispriced[['market_id', 'question', 'displayed_probability', 
                        'calibrated_probability', 'adjustment']].to_string(index=False))
    else:
        print("No significantly mispriced markets found.")
    
    # Example 4: Analyze adjustments across grid
    print("\n4. Generating Calibration Adjustment Heatmap")
    print("-" * 80)
    
    adjustments_df = analyze_calibration_adjustments(MODEL_PATH)
    
    print(f"Generated heatmap showing {len(adjustments_df)} probability × time combinations")
    
    # Example 5: Time decay analysis
    print("\n5. How Calibration Changes as Resolution Approaches")
    print("-" * 80)
    
    fixed_prob = 0.70
    time_points = [180, 90, 60, 30, 14, 7, 3, 1]
    
    print(f"\nFor a market showing {fixed_prob:.0%}:")
    print("\nDays Left | Calibrated | Adjustment")
    print("-" * 40)
    
    for days in time_points:
        pred = predict_calibrated_probability(
            model_path=MODEL_PATH,
            displayed_prob=fixed_prob,
            days_remaining=days,
            market_volume=100000
        )
        print(f"   {days:3d}    |   {pred['calibrated_probability']:.1%}    | {pred['adjustment']:+.1%}")
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
