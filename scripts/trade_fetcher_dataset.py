import os
import pandas as pd
import pyarrow.parquet as pq
from tqdm import tqdm

# --- config ---
PARQUET_PATH = r"C:\Users\Ed\OneDrive\Desktop\quant.parquet"
MARKET_ID    = "582457"   # set to None for ALL markets
HOURS_BACK   = 48

# If you want it auto-named:
OUT_PATH = (
    rf"C:\Users\Ed\OneDrive\Desktop\quant_last_{HOURS_BACK}h_{MARKET_ID}.csv"
    if MARKET_ID is not None
    else rf"C:\Users\Ed\OneDrive\Desktop\quant_last_{HOURS_BACK}h_ALL.csv"
)

# --- open parquet in streaming/chunk mode (row groups) ---
pf = pq.ParquetFile(PARQUET_PATH)
n_groups = pf.num_row_groups

# ---------- Pass 1: find latest datetime (after optional market filter) ----------
max_dt = None

for i in tqdm(range(n_groups), desc="Pass 1/2: finding latest timestamp", unit="group"):
    tbl = pf.read_row_group(i, columns=["datetime", "market_id"])
    chunk = tbl.to_pandas()

    chunk["datetime"] = pd.to_datetime(chunk["datetime"], errors="coerce")
    if MARKET_ID is not None:
        chunk = chunk[chunk["market_id"].astype(str) == str(MARKET_ID)]

    if chunk.empty:
        continue

    m = chunk["datetime"].max()
    if pd.notna(m) and (max_dt is None or m > max_dt):
        max_dt = m

if max_dt is None:
    raise ValueError("No rows found after market filter, or 'datetime' couldn't be parsed.")

cutoff = max_dt - pd.Timedelta(hours=HOURS_BACK)

print(f"Market filter: {MARKET_ID if MARKET_ID is not None else 'ALL'}")
print(f"Latest dt:     {max_dt}")
print(f"Cutoff dt:     {cutoff}")
print(f"Output:        {OUT_PATH}")

# ---------- Pass 2: filter + write CSV incrementally ----------
if os.path.exists(OUT_PATH):
    os.remove(OUT_PATH)

header_written = False
rows_written = 0

for i in tqdm(range(n_groups), desc="Pass 2/2: filtering & writing CSV", unit="group"):
    tbl = pf.read_row_group(i)          # read all columns in this row-group
    chunk = tbl.to_pandas()

    chunk["datetime"] = pd.to_datetime(chunk["datetime"], errors="coerce")
    if MARKET_ID is not None:
        chunk = chunk[chunk["market_id"].astype(str) == str(MARKET_ID)]

    chunk = chunk[chunk["datetime"].ge(cutoff)]

    if chunk.empty:
        continue

    # optional niceness: keep chronological order within each chunk
    chunk = chunk.sort_values("datetime")

    chunk.to_csv(
        OUT_PATH,
        mode="a",
        index=False,
        header=not header_written,
        date_format="%Y-%m-%d %H:%M:%S",
    )
    header_written = True
    rows_written += len(chunk)

print(f"Rows written:  {rows_written:,}")

