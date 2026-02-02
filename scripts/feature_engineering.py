import pandas as pd
import numpy as np
from fetch_trades_last import WINDOW_HOURS, MIN_TRADE_VOLUME

# LOAD & PREP

def load_trades(csv_path: str) -> pd.DataFrame:
    """
    Load Polymarket trades CSV and apply basic cleaning.
    """
    df = pd.read_csv(csv_path)

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values(["conditionId", "timestamp"]).reset_index(drop=True)

    return df


# MICROSTRUCTURE FEATURES

def add_trade_size_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Whale activity via relative trade size.
    """
    median_size = df.groupby("conditionId")["size"].transform("median")
    mean_size = df.groupby("conditionId")["size"].transform("mean")
    std_size = df.groupby("conditionId")["size"].transform("std").replace(0, np.nan)

    df["trade_size_ratio"] = df["size"] / median_size
    df["volume_zscore"] = (df["size"] - mean_size) / std_size

    return df


def add_flow_imbalance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Signed order flow imbalance proxy.
    """
    df["signed_volume"] = np.where(
        df["side"].str.lower() == "buy",
        df["size"],
        -df["size"]
    )

    total_flow = df.groupby("conditionId")["signed_volume"].transform("sum")
    total_volume = df.groupby("conditionId")["size"].transform("sum")

    df["flow_imbalance_ratio"] = total_flow / total_volume.replace(0, np.nan)

    return df


# PRICE ACTION FEATURES

def add_price_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Vertical price movement + realized volatility proxy.
    """
    price_mean = df.groupby("conditionId")["price"].transform("mean")
    price_std = df.groupby("conditionId")["price"].transform("std").replace(0, np.nan)

    df["price_zscore"] = (df["price"] - price_mean) / price_std

    df["log_price"] = np.log(df["price"])
    df["log_return"] = df.groupby("conditionId")["log_price"].diff()

    df["realized_volatility"] = (
        df.groupby("conditionId")["log_return"]
          .transform(lambda x: np.sqrt(np.nansum(x ** 2)))
    )

    return df


def add_amihud_illiquidity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Amihud price impact proxy.
    """
    df["abs_return"] = df["log_return"].abs()
    df["amihud"] = df["abs_return"] / df["size"].replace(0, np.nan)

    df["market_amihud"] = (
        df.groupby("conditionId")["amihud"]
          .transform("mean")
    )

    return df


# TEMPORAL / BEHAVIORAL FEATURES

def add_time_gap_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Urgency / clustering detection.
    """
    df["time_gap"] = (
        df.groupby("conditionId")["timestamp"]
          .diff()
          .dt.total_seconds()
    )

    df["log_time_gap"] = np.log1p(df["time_gap"])

    df["wallet_time_gap"] = (
        df.groupby(["conditionId", "proxyWallet"])["timestamp"]
          .diff()
          .dt.total_seconds()
    )

    return df


def add_wallet_experience_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    New / low-experience wallet detection.
    """
    df["is_new_wallet"] = (df["user_total_trades"] <= 1).astype(int)
    df["low_experience_wallet"] = (df["user_total_trades"] < 5).astype(int)

    return df


# PIPELINE

def build_feature_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full feature engineering pipeline.
    """
    df = add_trade_size_features(df)
    df = add_flow_imbalance(df)
    df = add_price_features(df)
    df = add_amihud_illiquidity(df)
    df = add_time_gap_features(df)
    df = add_wallet_experience_flags(df)

    return df


def get_model_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns clean ML-ready matrix for anomaly detection.
    """
    features = [
        "trade_size_ratio",
        "volume_zscore",
        "price_zscore",
        "realized_volatility",
        "market_amihud",
        "flow_imbalance_ratio",
        "log_time_gap",
        "is_new_wallet",
        "low_experience_wallet",
        "user_total_value",
        "user_total_trades"
    ]

    X = (
        df[features]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )

    return X


def run_feature_engineering(input_csv: str, output_csv: str):
    """
    End-to-end feature generation.
    """
    df = load_trades(input_csv)
    df = build_feature_matrix(df)

    df.to_csv(output_csv, index=False)
    print(f"✅ Features written to {output_csv}")


if __name__ == "__main__":
    run_feature_engineering(
        input_csv=r"C:\Users\2same\Economics BSc\Quant\PolyQuant\data\trades_last_" + str(WINDOW_HOURS) + "h_min_" + str(MIN_TRADE_VOLUME['filterAmount']) + ".csv",
        output_csv=r"C:\Users\2same\Economics BSc\Quant\PolyQuant\data\trades_with_features.csv"
    )


