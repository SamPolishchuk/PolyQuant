import pandas as pd
import json

MARKET_FILE = "data/market_ids_insider_only.csv"

def safe_insider_flag(x):
    if pd.isna(x):
        return False
    try:
        return json.loads(x).get("insider_tradable", False)
    except (json.JSONDecodeError, TypeError):
        return False

# Load CSV
markets_df = pd.read_csv(MARKET_FILE)
initial_rows = len(markets_df)

# Compute insider flag
markets_df["insider_tradable"] = markets_df["insider_tradability_json"].apply(
    safe_insider_flag
)

# Filter ONLY insider-tradable markets
markets_df = markets_df[markets_df["insider_tradable"]]

# Stats
final_rows = len(markets_df)
deleted = initial_rows - final_rows

# Drop helper columns
markets_df = markets_df.drop(columns=["insider_tradability_json", "insider_tradable"])

# Save back to same file
markets_df.to_csv(MARKET_FILE, index=False)

print(f"✅ Insider-only filter applied")
print(f"🗑️  Removed {deleted} non-insider-tradable markets")
print(f"📊 Remaining markets: {final_rows}")
