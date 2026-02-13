
r'''

import os
import pandas as pd
import pyarrow.parquet as pq
from tqdm import tqdm

# --- config ---
PARQUET_PATH = r"C:\Users\Ed\OneDrive\Desktop\quant.parquet"
MARKET_ID    = "635246"   # set to None for ALL markets
HOURS_BACK   = None

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

print(f"Rows written:  {rows_written:,}") '''


import os
import pandas as pd
import pyarrow.parquet as pq
from tqdm import tqdm

# --- config ---
PARQUET_PATH = r"C:\Users\Ed\OneDrive\Desktop\quant.parquet"
MARKET_ID    = "582457"   # set to None for ALL markets

# Set to an int (e.g. 48) to filter last N hours, or None to disable time filtering
HOURS_BACK   = None

# Output name
hb_label = f"{HOURS_BACK}h" if HOURS_BACK is not None else "ALLTIME"
mid_label = MARKET_ID if MARKET_ID is not None else "ALL"
OUT_PATH = rf"C:\Users\Ed\OneDrive\Desktop\quant_{hb_label}_{mid_label}.csv"

pf = pq.ParquetFile(PARQUET_PATH)
n_groups = pf.num_row_groups

# ---------- optional Pass 1: find latest datetime (only needed if HOURS_BACK is set) ----------
cutoff = None
max_dt = None

if HOURS_BACK is not None:
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
else:
    print(f"Market filter: {MARKET_ID if MARKET_ID is not None else 'ALL'}")
    print("Time filter:   DISABLED (exporting all available rows)")

print(f"Output:        {OUT_PATH}")

# ---------- Pass 2: filter + write CSV incrementally ----------
if os.path.exists(OUT_PATH):
    os.remove(OUT_PATH)

header_written = False
rows_written = 0

for i in tqdm(
    range(n_groups),
    desc=("Pass 2/2: filtering & writing CSV" if HOURS_BACK is not None else "Exporting & writing CSV"),
    unit="group",
):
    tbl = pf.read_row_group(i)  # all columns
    chunk = tbl.to_pandas()

    chunk["datetime"] = pd.to_datetime(chunk["datetime"], errors="coerce")
    if MARKET_ID is not None:
        chunk = chunk[chunk["market_id"].astype(str) == str(MARKET_ID)]

    if cutoff is not None:
        chunk = chunk[chunk["datetime"].ge(cutoff)]

    if chunk.empty:
        continue

    # optional: chronological order within chunk (keeps file "mostly" ordered without huge memory use)
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

