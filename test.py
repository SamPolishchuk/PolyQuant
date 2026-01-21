import requests
import pandas as pd
import numpy as np
from params import USER

ACTIVITY_URL = "https://data-api.polymarket.com/activity"

def fetch_trades(limit=200):
    params = {
        "user": USER,
        "limit": limit
    }
    res = requests.get(ACTIVITY_URL, params=params)
    res.raise_for_status()
    df = pd.DataFrame(res.json())
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

def flag_new_wallets(df):
    first_trade = df.groupby("trader")["timestamp"].min()
    df["is_new_wallet"] = df["trader"].map(
        lambda x: first_trade[x] == df[df["trader"] == x]["timestamp"].iloc[0]
    )
    return df

def flag_large_trades(df):
    df["log_size"] = np.log1p(df["size"])
    mean = df["log_size"].mean()
    std = df["log_size"].std()

    df["is_large_trade"] = df["log_size"] > mean + 2 * std
    return df

def detect_price_jump(trades_df, price_df, horizon_hours=24):
    jumps = []

    for _, trade in trades_df.iterrows():
        future_prices = price_df[
            (price_df["t"] > trade["timestamp"]) &
            (price_df["t"] <= trade["timestamp"] + pd.Timedelta(hours=horizon_hours))
        ]

        if future_prices.empty:
            jumps.append(False)
            continue

        max_move = abs(future_prices["p"].iloc[-1] - trade["price"]) / trade["price"]
        jumps.append(max_move > 0.05)

    trades_df["price_jump_after"] = jumps
    return trades_df

def compute_insider_score(df):
    df["insider_score"] = (
        2 * df["is_new_wallet"].astype(int) +
        2 * df["is_large_trade"].astype(int) +
        3 * df["price_jump_after"].astype(int)
    )
    return df

def run_pipeline(price_df):
    trades = fetch_trades()
    trades = flag_new_wallets(trades)
    trades = flag_large_trades(trades)
    trades = detect_price_jump(trades, price_df)
    trades = compute_insider_score(trades)

    suspects = trades[trades["insider_score"] >= 5]
    return suspects.sort_values("insider_score", ascending=False)

if __name__ == "__main__":
    # Example price data
    price_data = {
        "t": pd.date_range(start="2023-01-01", periods=100, freq="H"),
        "p": np.random.rand(100) * 100
    }
    price_df = pd.DataFrame(price_data)

    suspects = run_pipeline(price_df)
    print(suspects)
