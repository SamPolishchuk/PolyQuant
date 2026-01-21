import requests
import pandas as pd
from params import LIMIT



# Take the first three IDs and query the data-api endpoint
print("\n--- Querying data-api for market IDs ---\n")

# Prepare market IDs for query (keep in memory, don't save to CSV)
data_url = "https://data-api.polymarket.com/trades"
market_ids_df = resolved_markets_df[['id', 'conditionId', 'question']].head(10)

# Build comma-separated list of condition IDs
market_list = ','.join(market_ids_df['conditionId'].astype(str))

params_data = {
    "market": market_list,
    "limit": LIMIT,
}

try:

    r_data = requests.get(data_url, params=params_data, timeout=30)
    r_data.raise_for_status()
    print(f"Markets: {market_list}")
    print(f"Status: {r_data.status_code}")
    response_data = r_data.json()
    print(f"Total records: {len(response_data) if isinstance(response_data, list) else 'N/A'}")
    print(f"Sample data: {response_data[:2] if isinstance(response_data, list) else response_data}")
    
    # Save response data to CSV
    if isinstance(response_data, list) and len(response_data) > 0:
        trades_df = pd.DataFrame(response_data)
        trades_df.to_csv('trades_data.csv', index=False)
        print(f"\nSaved {len(trades_df)} records to trades_data.csv")
except Exception as e:
    print(f"Error: {e}\n")