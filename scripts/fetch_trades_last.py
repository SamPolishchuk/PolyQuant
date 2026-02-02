import requests
import pandas as pd
import time
import os
from datetime import timedelta

"""
MAKE THE DATES FIXES
"""

# CONFIG

TRADES_URL = "https://data-api.polymarket.com/trades"
USER_VALUE_URL = "https://data-api.polymarket.com/value"
USER_TRADED_URL = "https://data-api.polymarket.com/traded"

MARKET_FILE = "data/market_ids_insider_only.csv"
CHECKPOINT_FILE = "data/trade_checkpoints.csv"

LIMIT = 100
MIN_LIFETIME_TRADES = 10
MIN_TRADES_HOURS = 5
MIN_TRADE_VOLUME = {"filterType": "CASH", "filterAmount": 1000}
SLEEP_SECONDS = 0.5
TIMEOUT = 30

WINDOW_HOURS = 48
TRADES_CSV = f"data/trades_last_{WINDOW_HOURS}h_min_{MIN_TRADE_VOLUME['filterAmount']}.csv"
DONE_COL = f"done_{WINDOW_HOURS}h_{MIN_TRADE_VOLUME['filterAmount']}"

iteration_counter = 0

# HELPERS

USER_CACHE = {}

def append_to_csv(df, path):
    if df.empty:
        return
    write_header = not os.path.exists(path)
    df.to_csv(path, mode="a", index=False, header=write_header)

def parse_timestamp(series):
    if series.max() > 1e12:
        return pd.to_datetime(series, unit="ms", utc=True)
    return pd.to_datetime(series, unit="s", utc=True)

def get_user_stats(wallet):
    if wallet in USER_CACHE:
        return USER_CACHE[wallet]

    stats = {"user_total_value": 0, "user_total_trades": 0}

    try:
        r_val = requests.get(USER_VALUE_URL, params={"user": wallet}, timeout=TIMEOUT)
        val_data = r_val.json()
        if val_data and isinstance(val_data, list):
            stats["user_total_value"] = val_data[0].get("value", 0)

        r_trd = requests.get(USER_TRADED_URL, params={"user": wallet}, timeout=TIMEOUT)
        trd_data = r_trd.json()
        stats["user_total_trades"] = trd_data.get("traded", 0)

    except Exception:
        pass

    USER_CACHE[wallet] = stats
    return stats

def log_csv_status(csv_path):
    if not os.path.exists(csv_path):
        print("    📄 CSV does not exist yet.")
        return

    try:
        df_tail = pd.read_csv(csv_path, usecols=["timestamp"])
        total_rows = len(df_tail)
        last_ts = pd.to_datetime(df_tail["timestamp"].iloc[-1], utc=True)

        print(f"    📊 CSV rows: {total_rows:,}")
        print(f"    🕒 Last appended trade: {last_ts}")

    except Exception as e:
        print(f"    ⚠ Could not read CSV status: {e}")


# CHECKPOINT HANDLING
if "__name__"=="__main__":
        
    if os.path.exists(CHECKPOINT_FILE):
        checkpoints = pd.read_csv(CHECKPOINT_FILE)
    else:
        checkpoints = pd.DataFrame(columns=["conditionId", "is_structurally_dead", DONE_COL])

    for col in ["is_structurally_dead", DONE_COL]:
        if col not in checkpoints.columns:
            checkpoints[col] = False

    dead_ids = set(checkpoints.loc[checkpoints["is_structurally_dead"], "conditionId"])
    done_ids = set(checkpoints.loc[checkpoints[DONE_COL], "conditionId"])

    # LOAD MARKETS

    markets_df = pd.read_csv(MARKET_FILE)
    markets_df = markets_df[~markets_df["conditionId"].isin(dead_ids | done_ids)]
    markets_df["closedTime"] = pd.to_datetime(markets_df["closedTime"], utc=True, errors="coerce")
    markets_df = markets_df.dropna(subset=["closedTime"])

    print(f"✅ Target File: {TRADES_CSV}")
    print(f"✅ Tracking via column: {DONE_COL}")
    print(f"✅ Ready to process {len(markets_df)} markets.")

    # MAIN LOOP

    total_appended = 0

    for _, market in markets_df.iterrows():
        question = market["question"]
        close_time = market["closedTime"]
        end_time = market["endDate"]
        condition_id = market["conditionId"]
        early = True

        if close_time > end_time:
            close_time = end_time
            early = False

        cutoff_time = close_time - timedelta(hours=WINDOW_HOURS)

        print(f"\n▶ Market: {question}")

        offset = 0
        done_this_window = False
        structurally_dead = False

        while True:
            time.sleep(SLEEP_SECONDS)

            try:
                r = requests.get(
                    TRADES_URL,
                    params={"limit": LIMIT, "offset": offset, "market": condition_id, "filterType": MIN_TRADE_VOLUME['filterType'], "filterAmount": MIN_TRADE_VOLUME['filterAmount']},
                    timeout=TIMEOUT
                )
                r.raise_for_status()
                data = r.json()

                if not data:
                    done_this_window = True
                    if offset == 0:
                        print(f"  No data available for trades greater than {MIN_TRADE_VOLUME['filterAmount']}. Set done_this_window = True")
                        structurally_dead = True
                    break

                iteration_counter += 1

                if iteration_counter % 50 == 0:
                    print("\n🔎 Progress checkpoint")
                    log_csv_status(TRADES_CSV)


                df = pd.DataFrame(data)
                df["timestamp"] = parse_timestamp(df["timestamp"])
                # TRADE-HISTORY AT LEAST 50 TRANSACTIONS
                if offset == 0:
                    if len(data) < MIN_LIFETIME_TRADES:
                        print(f"  ! Only {len(data)} lifetime trades (<{MIN_LIFETIME_TRADES}). Dropping market.")
                        structurally_dead = True
                        done_this_window = True
                        break

                keep = df[df["timestamp"] >= cutoff_time].copy()

                if not keep.empty:

                    if len(keep) < MIN_TRADES_HOURS:
                        if offset == 0:
                            print(f"    ! Only {len(keep)} trades in the last {WINDOW_HOURS}h (<{MIN_TRADES_HOURS}). Skipping these trades.")
                            structurally_dead = True
                            done_this_window = True
                            break

                    users = keep["proxyWallet"].unique()
                    user_map = {u: get_user_stats(u) for u in users}

                    keep["user_total_value"] = keep["proxyWallet"].map(lambda x: user_map[x]["user_total_value"])
                    keep["user_total_trades"] = keep["proxyWallet"].map(lambda x: user_map[x]["user_total_trades"])
                    keep["conditionId"] = condition_id

                    append_to_csv(keep, TRADES_CSV)
                    total_appended += len(keep)

                    print(f"    Added {len(keep)} trades greater than {MIN_TRADE_VOLUME['filterAmount']}.")

                if df["timestamp"].min() <= cutoff_time:
                    done_this_window = True
                    if keep.empty and offset == 0:
                        print(f"  No more trades in the last {WINDOW_HOURS}h. Set done_this_window = True")
                    break

                offset += LIMIT

            except Exception as e:
                print(f"  Error: {e}")
                time.sleep(5)

        # UPDATE CHECKPOINTS
        row = {
            "conditionId": condition_id,
            "is_structurally_dead": structurally_dead,
            DONE_COL: done_this_window
        }

        checkpoints = checkpoints[checkpoints["conditionId"] != condition_id]
        checkpoints = pd.concat([checkpoints, pd.DataFrame([row])], ignore_index=True)
        checkpoints.to_csv(CHECKPOINT_FILE, index=False)

    print(f"\n✅ Scraping finished. Total rows added: {total_appended}")
