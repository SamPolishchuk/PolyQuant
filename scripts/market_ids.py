import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os

url = "https://gamma-api.polymarket.com/markets"
csv_file = r"C:\Users\2same\Economics BSc\Quant\PolyQuant\data\market_id.csv"

#test
LIMIT = 500

# Initialize CSV if it doesn't exist or is empty
if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
    try:
        existing_ids = set(pd.read_csv(csv_file)['conditionId'].astype(str))
        print(f"Found existing CSV with {len(existing_ids)} markets\n")
    except pd.errors.EmptyDataError:
        existing_ids = set()
        print(f"CSV file is empty, starting fresh\n")
else:
    existing_ids = set()
    print(f"Creating new CSV file: {csv_file}\n")

# Start from 2021-01-01, slide by weeks
start_date = datetime(2026, 1, 1)
end_date = datetime.now()
current_date = start_date
week_duration = timedelta(days=1)

all_markets = []

while current_date < end_date:
    time.sleep(1)  # Be polite to the API

    week_end = current_date + week_duration
    
    # Ensure we don't go beyond today
    if week_end > end_date:
        week_end = end_date
    
    # Format dates for API
    end_date_min = current_date.isoformat() + "Z"
    end_date_max = week_end.isoformat() + "Z"
    
    week_num = (current_date - start_date).days // 7 + 1
    print(f"\n=== Week {week_num}: {current_date.date()} to {week_end.date()} ===")
    print(f"Querying: {end_date_min} to {end_date_max}")
    
    params = {
        "active": False,
        "closed": True,
        "limit": LIMIT,
        "volume_num_min": 150000,
        "order": "endDate",
        "end_date_max": end_date_max,
        "end_date_min": end_date_min,
        "ascending": True,
    }
    
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        
        df = pd.DataFrame(r.json())
        
        if len(df) > 0:
            df['endDate'] = pd.to_datetime(df['endDate'], format='ISO8601')
            
            # Filter for resolved markets
            resolved_df = df[df['endDate'] < pd.Timestamp.now(tz='UTC')]
            
            # Filter out markets already in CSV
            new_markets = resolved_df[~resolved_df['conditionId'].astype(str).isin(existing_ids)]
            
            if len(new_markets) > 0:
                all_markets.append(new_markets)
                existing_ids.update(new_markets['conditionId'].astype(str))
                print(f"  Found {len(new_markets)} new markets (total existing: {len(existing_ids)})")
                
                # Save to CSV at each iteration
                combined_df = pd.concat(all_markets, ignore_index=True)
                if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
                    existing_df = pd.read_csv(csv_file)
                    combined_df = pd.concat([existing_df, combined_df], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset=['conditionId'], keep='first')
                combined_df.to_csv(csv_file, index=False)
                print(f"  Saved {len(combined_df)} total markets to {csv_file}")
            else:
                print(f"  No new markets in this period")
        else:
            print(f"  No markets found")
            
    except Exception as e:
        print(f"  Error: {e}")
    
    # Move to next week
    current_date = week_end

print("\nScraping complete!")