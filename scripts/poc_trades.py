# POLYMARKET — CHUNKED, RESUMABLE TRADE SCRAPER (24H)

import requests
import pandas as pd
import time
import os
from datetime import timedelta

# ======================================================
# CONFIG
# ======================================================

MARKETS_FILE = r"C:\Users\2same\Economics BSc\Quant\PolyQuant\data\poc_markets.csv"
OUTPUT_DIR = r"C:\Users\2same\Economics BSc\Quant\PolyQuant\data\poc_trades"

os.makedirs(OUTPUT_DIR, exist_ok=True)

TRADES_URL = "https://data-api.polymarket.com/trades"

WINDOW_HOURS = 24
LIMIT = 100
CHUNK_SIZE = 3000

SLEEP_SECONDS = 0.5
TIMEOUT = 30

# ======================================================
# HELPERS
# ======================================================

def parse_timestamp(series):
    if series.max() > 1e12:
        return pd.to_datetime(series, unit="ms", utc=True)
    return pd.to_datetime(series, unit="s", utc=True)

def flush_chunk(buffer, out_file):
    if not buffer:
        return 0

    df = pd.concat(buffer, ignore_index=True)
    write_header = not os.path.exists(out_file)
    df.to_csv(out_file, mode="a", index=False, header=write_header)
    buffer.clear()
    return len(df)

# ======================================================
# MAIN
# ======================================================

def main():

    markets = pd.read_csv(MARKETS_FILE)

    markets["closedTime"] = pd.to_datetime(markets["closedTime"], utc=True, errors="coerce")
    markets["endDate"] = pd.to_datetime(markets["endDate"], utc=True, errors="coerce")
    markets = markets.dropna(subset=["conditionId", "closedTime", "endDate"])

    print(f"🚀 Found {len(markets)} markets")

    for _, market in markets.iterrows():

        condition_id = market["conditionId"]
        close_time = min(market["closedTime"], market["endDate"])
        cutoff_time = close_time - timedelta(hours=WINDOW_HOURS)

        out_file = f"{OUTPUT_DIR}/trades_{condition_id}.csv"

        print(f"\n▶ {condition_id}")

        chunk_buffer = []
        total_saved = 0
        offset = 0

        oldest_seen_ts = None
        stagnant_pages = 0
        last_page_signature = None

        while True:
            time.sleep(SLEEP_SECONDS)

            try:
                r = requests.get(
                    TRADES_URL,
                    params={
                        "market": condition_id,
                        "limit": LIMIT,
                        "offset": offset,
                    },
                    timeout=TIMEOUT,
                )
                r.raise_for_status()
                data = r.json()

                if not data:
                    break

                df = pd.DataFrame(data)
                df["timestamp"] = parse_timestamp(df["timestamp"])

                # only keep last 24h
                df = df[df["timestamp"] >= cutoff_time]

                if df.empty:
                    break

                # --------------------------------------------------
                # 🔁 Repeated page detection (robust, no tradeId)
                # --------------------------------------------------
                page_signature = (
                    df["proxyWallet"].iloc[-1],
                    df["timestamp"].iloc[-1],
                    len(df),
                )

                if last_page_signature == page_signature:
                    print("   🔁 Repeated page detected — terminating market")
                    break

                last_page_signature = page_signature

                # --------------------------------------------------
                # ⏱ Timestamp progress detection (FIXED)
                # --------------------------------------------------
                current_min_ts = df["timestamp"].min()

                if oldest_seen_ts is None:
                    oldest_seen_ts = current_min_ts
                    stagnant_pages = 0
                elif current_min_ts < oldest_seen_ts:
                    oldest_seen_ts = current_min_ts
                    stagnant_pages = 0
                else:
                    stagnant_pages += 1

                if stagnant_pages >= 3:
                    print("   ⛔ No timestamp progress — terminating market")
                    break

                # --------------------------------------------------
                # Save logic
                # --------------------------------------------------
                chunk_buffer.append(df)
                offset += LIMIT

                if sum(len(x) for x in chunk_buffer) >= CHUNK_SIZE:
                    flushed = flush_chunk(chunk_buffer, out_file)
                    total_saved += flushed

                    delta = close_time - oldest_seen_ts
                    print(
                        f"   ⏱ Progress: {total_saved:,} trades | "
                        f"closeTime - oldest trade = {delta}"
                    )

                # crossed the 24h window
                if current_min_ts <= cutoff_time:
                    break

            except Exception as e:
                print(f"⚠ API error: {e}")
                time.sleep(5)
                break

        # final flush
        flushed = flush_chunk(chunk_buffer, out_file)
        total_saved += flushed

        print(f"   ✅ Done — {total_saved:,} trades saved")

    print("\n🎉 ALL MARKETS COMPLETE")

# ======================================================
# RUN
# ======================================================

if __name__ == "__main__":
    main()
