import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from scipy.stats import percentileofscore

#to do:
#figure out what the traders are buying, Yes tokens or No, so we know which side of market they are on, look at documentation on github for huggins.
#look at features, think of what features we could add.
#add other potential insider markets, e.g. the other google-related markets the AlphaRaccoon bet on.

#notes:
#for detect_jump the rolling window is hardcoded to 2 hours.

# ===============================
# 1. Load metadata
# ===============================
metadata = pd.read_csv("trades_details.csv")
print(metadata.head())

# ===============================
# 2. Helper functions
# ===============================

def detect_jump(df):
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime")
    df = df.set_index("datetime")
    
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("Index is not DatetimeIndex")
    
    df["price_change"] = df["price"].rolling("2h").apply(lambda x: x.iloc[-1] - x.iloc[0], raw=False)
    df["abs_move"] = df["price_change"].abs()
    
    idx = df["abs_move"].idxmax()
    return idx

def build_wallet_profiles(trades, original_data, jump_time=None):
    market_vwap = (original_data["price"] * original_data["usd_amount"]).sum() / (original_data["usd_amount"].sum() + 1e-9)
    total_market_volume = original_data["usd_amount"].sum()
    
    profiles = trades.groupby("wallet").agg({
        "usd_amount": ["sum", "count", "max", "std"],
        "price": "mean",
        "datetime": ["min", "max"],
        "role": lambda x: (x == "maker").sum()
    })
    
    profiles.columns = [
        "total_volume",
        "trade_count",
        "max_trade",
        "trade_std",
        "avg_price",
        "first_seen",
        "last_seen",
        "maker_trades"
    ]
    
    profiles["first_seen"] = pd.to_datetime(profiles["first_seen"])
    profiles["last_seen"] = pd.to_datetime(profiles["last_seen"])
    profiles["maker_ratio"] = profiles["maker_trades"] / profiles["trade_count"]
    profiles["price_edge"] = market_vwap - profiles["avg_price"]
    
    if jump_time is not None:
        profiles["lead_time_seconds"] = (jump_time - profiles["first_seen"]).dt.total_seconds()
    else:
        profiles["lead_time_seconds"] = 0
        
    profiles["conviction_ratio"] = profiles["max_trade"] / (profiles["total_volume"] + 1e-9)
    profiles["fraction_of_market_volume"] = profiles["total_volume"] / (total_market_volume + 1e-9)
    
    return profiles.fillna(0)

# ===============================
# 3. Aggregate wallet stats across all markets
# ===============================
all_wallet_stats = []

for _, row in metadata.iterrows():
    file_name = row["file_name"]
    event_name = row["event_name"]
    insider_address = row["insider_address"].strip().lower()

    data = pd.read_csv(file_name)

    # Handle timestamp
    if "timestamp" in data.columns:
        data["datetime"] = pd.to_datetime(data["timestamp"], unit="s")
    else:
        data["datetime"] = pd.to_datetime(data["datetime"])

    # Normalize addresses
    data["maker"] = data["maker"].str.strip().str.lower()
    data["taker"] = data["taker"].str.strip().str.lower()

    # Build maker/taker
    maker_df = data.copy()
    maker_df["wallet"] = maker_df["maker"]
    maker_df["role"] = "maker"

    taker_df = data.copy()
    taker_df["wallet"] = taker_df["taker"]
    taker_df["role"] = "taker"

    trades = pd.concat([maker_df, taker_df], ignore_index=True)
    trades = trades[["wallet", "role", "datetime", "price", "usd_amount", "nonusdc_side"]]

    # Detect jump
    jump_time = detect_jump(data)

    # Build wallet profiles
    wallet_stats = build_wallet_profiles(trades, data, jump_time)
    wallet_stats["event_name"] = event_name
    wallet_stats["insider_address"] = insider_address
    wallet_stats["is_insider"] = wallet_stats.index == insider_address

    all_wallet_stats.append(wallet_stats)

# Combine all events
combined_stats = pd.concat(all_wallet_stats, axis=0)
print("Combined wallet stats shape:", combined_stats.shape)

# ===============================
# 4. Scale features across all markets
# ===============================
feature_cols = [
    "avg_price",
    "total_volume",
    "price_edge",
    "lead_time_seconds",
    "maker_ratio",
    "conviction_ratio",
    "fraction_of_market_volume"
]

scaler = MinMaxScaler()
combined_stats_scaled = combined_stats.copy()
combined_stats_scaled[feature_cols] = scaler.fit_transform(combined_stats[feature_cols])

# ===============================
# 5. Shared scatterplots
# ===============================
feature_pairs = [
    ("avg_price", "total_volume"),
    ("price_edge", "lead_time_seconds"),
    ("maker_ratio", "conviction_ratio"),
    ("fraction_of_market_volume", "total_volume"),
    ("avg_price", "price_edge"),
    ("lead_time_seconds", "conviction_ratio"),
    ("maker_ratio", "fraction_of_market_volume")
]

n_pairs = len(feature_pairs)
cols = 3
rows = (n_pairs // cols) + (n_pairs % cols > 0)

fig, axes = plt.subplots(rows, cols, figsize=(18, rows * 5))
axes = axes.flatten()

for i, (x_feature, y_feature) in enumerate(feature_pairs):
    ax = axes[i]
    
    # Population (non-insiders)
    population = combined_stats_scaled[~combined_stats_scaled["is_insider"]]
    ax.scatter(
        population[x_feature],
        population[y_feature],
        alpha=0.3,
        color="skyblue",
        s=50,
        zorder=1,
        label="Population"
    )
    
    # Insiders
    insiders = combined_stats_scaled[combined_stats_scaled["is_insider"]]
    ax.scatter(
        insiders[x_feature],
        insiders[y_feature],
        color="red",
        s=150,
        edgecolor="black",
        label="Insider",
        zorder=5
    )
    
    ax.set_xlabel(x_feature)
    ax.set_ylabel(y_feature)
    ax.set_title(f"{x_feature} vs {y_feature} (All Markets)")
    ax.grid(True)
    ax.legend()

# Remove empty subplots
for j in range(i + 1, len(axes)):
    fig.delaxes(axes[j])

plt.tight_layout()
plt.show()
