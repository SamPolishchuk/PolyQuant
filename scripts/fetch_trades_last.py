import requests
import pandas as pd
import time
import os
from datetime import timedelta

# ======================================================
# CONFIG
# ======================================================

TRADES_URL = "https://data-api.polymarket.com/trades"
USER_VALUE_URL = "https://data-api.polymarket.com/value"
USER_TRADED_URL = "https://data-api.polymarket.com/traded"

MARKET_FILE = "data/market_ids_cleaned.csv"
CHECKPOINT_FILE = "data/trade_checkpoints.csv"

# --- GLOBAL CONSTANTS ---
WINDOW_HOURS = 72  
TRADES_CSV = f"data/trades_last_{WINDOW_HOURS}h.csv"
DONE_COL = f"done_{WINDOW_HOURS}h" # Dynamic column name

LIMIT = 100
SLEEP_SECONDS = 0.5
TIMEOUT = 30

# ======================================================
# HELPERS
# ======================================================

def append_to_csv(df, path):
    if df.empty: return
    write_header = not os.path.exists(path)
    df.to_csv(path, mode="a", index=False, header=write_header)

def parse_timestamp(series):
    if series.max() > 1e12:
        return pd.to_datetime(series, unit="ms", utc=True)
    return pd.to_datetime(series, unit="s", utc=True)

def get_user_stats(proxy_wallet):
    stats = {"user_total_value": 0, "user_total_trades": 0}
    try:
        val_r = requests.get(USER_VALUE_URL, params={"user": proxy_wallet}, timeout=TIMEOUT)
        val_data = val_r.json()
        if val_data and isinstance(val_data, list):
            stats["user_total_value"] = val_data[0].get("value", 0)

        trd_r = requests.get(USER_TRADED_URL, params={"user": proxy_wallet}, timeout=TIMEOUT)
        trd_data = trd_r.json()
        stats["user_total_trades"] = trd_data.get("traded", 0)
    except Exception:
        pass 
    return stats

# ======================================================
# INITIALIZE CHECKPOINTS
# ======================================================

if os.path.exists(CHECKPOINT_FILE):
    checkpoints = pd.read_csv(CHECKPOINT_FILE)
    # Ensure our dynamic column and is_empty exist
    if DONE_COL not in checkpoints.columns:
        checkpoints[DONE_COL] = False
    if "is_empty" not in checkpoints.columns:
        checkpoints["is_empty"] = False
else:
    checkpoints = pd.DataFrame(columns=["conditionId", "offset", "is_empty", DONE_COL])

# Remove empty markets from the main file reference
empty_market_ids = set(checkpoints.loc[checkpoints["is_empty"] == True, "conditionId"])

markets_df = pd.read_csv(MARKET_FILE)
original_count = len(markets_df)
markets_df = markets_df[~markets_df["conditionId"].isin(empty_market_ids)]

if len(markets_df) < original_count:
    print(f"🗑️  Pruned {original_count - len(markets_df)} empty markets from {MARKET_FILE}")
    markets_df.to_csv(MARKET_FILE, index=False)

# Prepare active list: Not empty AND not done for THIS specific window
completed_ids = set(checkpoints.loc[checkpoints[DONE_COL] == True, "conditionId"])
active_markets = markets_df[
    ~markets_df["conditionId"].isin(completed_ids) & 
    ~markets_df["conditionId"].isin(empty_market_ids)
].copy()

active_markets["closedTime"] = pd.to_datetime(active_markets["closedTime"], utc=True, errors="coerce")
active_markets = active_markets.dropna(subset=["closedTime"])

checkpoint_map = {row.conditionId: row for _, row in checkpoints.iterrows()}

print(f"✅ Target File: {TRADES_CSV}")
print(f"✅ Tracking via column: {DONE_COL}")
print(f"✅ Ready to process {len(active_markets)} markets.")

# ======================================================
# MAIN SCRAPING LOOP
# ======================================================

total_appended = 0

for _, market in active_markets.iterrows():
    condition_id = market["conditionId"]
    close_time = market["closedTime"]
    cutoff_time = close_time - timedelta(hours=WINDOW_HOURS)

    state = checkpoint_map.get(condition_id, None)
    # We reset offset to 0 if we are starting a brand new window duration
    offset = 0 
    done_this_window = False
    is_empty = False

    print(f"\n▶ Market {condition_id}")
    
    while True:
        time.sleep(SLEEP_SECONDS)
        try:
            r = requests.get(TRADES_URL, params={"limit": LIMIT, "offset": offset, "market": condition_id}, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()

            if not data:
                if offset == 0:
                    print("  ! No data found. Flagging as is_empty.")
                    is_empty = True
                done_this_window = True
                break

            df = pd.DataFrame(data)
            df["timestamp"] = parse_timestamp(df["timestamp"])
            
            keep = df[df["timestamp"] >= cutoff_time].copy()
            
            if not keep.empty:
                unique_users = keep["proxyWallet"].unique()
                user_map = {u: get_user_stats(u) for u in unique_users}
                
                keep["user_total_value"] = keep["proxyWallet"].map(lambda x: user_map[x]["user_total_value"])
                keep["user_total_trades"] = keep["proxyWallet"].map(lambda x: user_map[x]["user_total_trades"])
                keep["conditionId"] = condition_id
                
                append_to_csv(keep, TRADES_CSV)
                total_appended += len(keep)
                print(f"    Added {len(keep)} trades to {os.path.basename(TRADES_CSV)}")
            else:
                print(f"    Found {len(df)} trades, but all are older than {WINDOW_HOURS}h.")

            if df["timestamp"].min() <= cutoff_time:
                done_this_window = True
                break
            offset += LIMIT

        except Exception as e:
            print(f"  Error: {e}")
            time.sleep(5)
            continue

    # Update the single checkpoint file
    # If the market already exists in checkpoints, update it; else, add it.
    if condition_id in checkpoints["conditionId"].values:
        idx = checkpoints.index[checkpoints["conditionId"] == condition_id][0]
        checkpoints.at[idx, DONE_COL] = done_this_window
        checkpoints.at[idx, "is_empty"] = is_empty
    else:
        new_row = {"conditionId": condition_id, "offset": offset, "is_empty": is_empty, DONE_COL: done_this_window}
        checkpoints = pd.concat([checkpoints, pd.DataFrame([new_row])], ignore_index=True)
    
    checkpoints.to_csv(CHECKPOINT_FILE, index=False)

print(f"\n✅ Scraping finished. Total rows added: {total_appended}")