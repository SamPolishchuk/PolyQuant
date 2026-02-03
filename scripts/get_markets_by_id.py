import requests
import pandas as pd
import time
import os

# ======================================================
# CONFIG
# ======================================================

MARKETS_URL = "https://gamma-api.polymarket.com/markets"
MARKET_IDS =["0xc25e15f39f776813870ee363ed483451add1c55dad163b18bdb2df653be2c90c", "0x580adc1327de9bf7c179ef5aaffa3377bb5cb252b7d6390b027172d43fd6f993", "0x7f39808829da93cfd189807f13f6d86a0e604835e6f9482d8094fac46b3abaac"]

OUTPUT_FILE = r"C:\Users\2same\Economics BSc\Quant\PolyQuant\data\poc_markets.csv"

SLEEP_SECONDS = 0.8
TIMEOUT = 30

KEEP_COLS = [
    "id",
    "question",
    "model_text",
    "outcomes",
    "outcomePrices",
    "conditionId",
    "slug",
    "endDate",
    "closedTime",
]

# ======================================================
# LOAD INPUT MARKET LIST
# ======================================================

print(f"Loaded {len(MARKET_IDS)} predetermined markets")

# ======================================================
# LOAD EXISTING OUTPUT (CHECKPOINT)
# ======================================================

if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
    existing_df = pd.read_csv(OUTPUT_FILE)
    seen_ids = set(existing_df["conditionId"].astype(str))
    print(f"Found existing output with {len(seen_ids)} markets")
else:
    existing_df = pd.DataFrame()
    seen_ids = set()
    print("Starting fresh output file")

# ======================================================
# MAIN LOOP
# ======================================================

rows = []

for i, condition_id in enumerate(MARKET_IDS, 1):

    if condition_id in seen_ids:
        continue

    time.sleep(SLEEP_SECONDS)

    print(f"[{i}/{len(MARKET_IDS)}] Fetching {condition_id}")

    params = {
        "condition_ids": condition_id
    }

    try:
        r = requests.get(MARKETS_URL, params=params, timeout=TIMEOUT)
        r.raise_for_status()

        data = r.json()

        if not data:
            print("  No data returned")
            continue

        df = pd.DataFrame(data)

        # Keep only requested columns (if present)
        df = df[[c for c in KEEP_COLS if c in df.columns]]

        rows.append(df)

        # Incremental save
        combined = pd.concat([existing_df] + rows, ignore_index=True)
        combined = combined.drop_duplicates(subset=["conditionId"], keep="first")
        combined.to_csv(OUTPUT_FILE, index=False)

        existing_df = combined
        seen_ids.add(condition_id)

        print(f"  Saved ({len(existing_df)} total markets)")

    except Exception as e:
        print(f"  Error fetching {condition_id}: {e}")

print("\nDone.")
