import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from scipy.stats import norm, percentileofscore, shapiro, skew, kurtosis

metadata = pd.read_csv("trades_details.csv")

print(metadata.head())

for _, row in metadata.iterrows():
    file_name = row["file_name"]
    event_name = row["event_name"]
    insider_address = row["insider_address"]
    insider_address = insider_address.lower()

    data = pd.read_csv(file_name)

    # If you have 'timestamp' column, convert it to datetime
    if "timestamp" in data.columns:
        data["datetime"] = pd.to_datetime(data["timestamp"], unit="s")
    else:
        data["datetime"] = pd.to_datetime(data["datetime"])


    # Normalize addresses before building wallet columns
    data["maker"] = data["maker"].str.strip().str.lower()
    data["taker"] = data["taker"].str.strip().str.lower()
    insider_address = insider_address.strip().lower()

    # Build maker/taker
    maker_df = data.copy()
    maker_df["wallet"] = maker_df["maker"]
    maker_df["role"] = "maker"

    taker_df = data.copy()
    taker_df["wallet"] = taker_df["taker"]
    taker_df["role"] = "taker"

    # Stack together
    trades = pd.concat([maker_df, taker_df], ignore_index=True)

    trades = trades[
    ["wallet", "role", "datetime", "price", "usd_amount", "nonusdc_side"]
    ]

    # ===============================
    # 3. BUILD WALLET PROFILES
    # ===============================


    def detect_jump(df):

        df = df.copy()
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime")
        df = df.set_index("datetime")

        # IMPORTANT: ensure index really is datetime
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("Index is not DatetimeIndex")

        df["price_change"] = (
            df["price"]
            .rolling("2h")   # capital H safer
            .apply(lambda x: x.iloc[-1] - x.iloc[0], raw=False)
        )

        df["abs_move"] = df["price_change"].abs()

        idx = df["abs_move"].idxmax()

        return idx


    jump_time = detect_jump(data)
    print("Jump time:", jump_time)

    def build_wallet_profiles(trades, original_data, jump_time):

        market_vwap = (
            (original_data["price"] * original_data["usd_amount"]).sum()
            / (original_data["usd_amount"].sum() + 1e-9)
        )

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


        profiles["maker_ratio"] = (
            profiles["maker_trades"] / profiles["trade_count"]
        )

        profiles["price_edge"] = market_vwap - profiles["avg_price"]

        if jump_time is not None:
            profiles["lead_time_seconds"] = (
                jump_time - profiles["first_seen"]
            ).dt.total_seconds()
        else:
            profiles["lead_time_seconds"] = 0

        profiles["conviction_ratio"] = (
            profiles["max_trade"] / (profiles["total_volume"] + 1e-9)
        )

        profiles["fraction_of_market_volume"] = (
            profiles["total_volume"] / (total_market_volume + 1e-9)
        )

        return profiles.fillna(0)

    wallet_stats = build_wallet_profiles(trades, data, jump_time)


    if insider_address not in wallet_stats.index:
        print("Available wallets:", wallet_stats.index[:10])
        raise ValueError(f"Insider wallet {insider_address} not found in dataset.")


    # ===============================
    # 4. FEATURE DEVIATION ANALYSIS (Percentile-Based)
    # ===============================
    # Updated feature columns including new metrics
    feature_cols = [
        "avg_price",
        "total_volume",
        "price_edge",
        "lead_time_seconds",
        "maker_ratio",
        "conviction_ratio",
        "fraction_of_market_volume"
    ]


    # Remove insider to compute population stats
    population = wallet_stats.drop(index=insider_address)

    results = []

    for feature in feature_cols:
        insider_value = wallet_stats.loc[insider_address, feature]
        mean = population[feature].mean()
        
        # Percentile rank
        percentile = percentileofscore(population[feature], insider_value, kind="mean")
        
        results.append({
            "Feature": feature,
            "Insider Value": insider_value,
            "Population Mean": mean,
            "Percentile Rank (%)": percentile
        })

    # Sort by percentile so top features stand out
    feature_deviation = pd.DataFrame(results).sort_values("Percentile Rank (%)", ascending=False)

    print("\nFeatures That Give the Trader Away (Percentile-Based) for {}:".format(event_name))
    print(feature_deviation.to_string(index=False))


    population = wallet_stats.drop(index=insider_address)
    insider = wallet_stats.loc[insider_address]

        # Feature columns to analyze
    feature_pairs = [
    ("avg_price", "total_volume"),
    ("price_edge", "lead_time_seconds"),
    ("maker_ratio", "conviction_ratio"),
    ("fraction_of_market_volume", "total_volume"),
    ("avg_price", "price_edge"),
    ("lead_time_seconds", "conviction_ratio"),
    ("maker_ratio", "fraction_of_market_volume")
    ]

    # Determine subplot grid size
    n_pairs = len(feature_pairs)
    cols = 3
    rows = (n_pairs // cols) + (n_pairs % cols > 0)

    fig, axes = plt.subplots(rows, cols, figsize=(18, rows * 5))
    axes = axes.flatten()

    # Optionally scale features to 0-1 to make visualization clearer
    scaler = MinMaxScaler()
    population_scaled = pd.DataFrame(
        scaler.fit_transform(population[feature_cols]),
        columns=feature_cols,
        index=population.index
    )
    insider_scaled = pd.DataFrame(
        scaler.transform(insider[feature_cols].to_frame().T),
        columns=feature_cols,
        index=[insider.name]
    )

    for i, (x_feature, y_feature) in enumerate(feature_pairs):
        ax = axes[i]
        
        # Plot population (light blue, semi-transparent)
        ax.scatter(
            population_scaled[x_feature],
            population_scaled[y_feature],
            alpha=0.5,
            label="Population",
            color="skyblue",
            s=50,
            zorder=1
        )
        
        # Plot insider (red, large, on top)
        ax.scatter(
            insider_scaled.loc[insider.name, x_feature],
            insider_scaled.loc[insider.name, y_feature],
            color="red",
            s=200,
            edgecolor="black",
            label="Insider",
            zorder=5
        )
        
        # Annotate insider
        ax.annotate(
            "Insider",
            (insider_scaled.loc[insider.name, x_feature],
            insider_scaled.loc[insider.name, y_feature]),
            xytext=(5, 5),
            textcoords='offset points',
            color='red',
            fontsize=12,
            fontweight='bold'
        )
        
        ax.set_xlabel(x_feature)
        ax.set_ylabel(y_feature)
        ax.set_title(f"{x_feature} vs {y_feature}")
        ax.grid(True)
        ax.legend()

    # Remove any empty subplots
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    plt.tight_layout()
    plt.show()