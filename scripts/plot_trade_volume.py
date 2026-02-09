# Total number of trades over time (ALL markets) from your parquet files.
# Paste this into a notebook / Python file in the same environment where the parquets live and run.

import duckdb
import pandas as pd
import matplotlib.pyplot as plt

# ---- config ----
PARQUET_GLOB = r"C:\Users\Ed\OneDrive\Desktop\quant.parquet"    # change if needed
TS_COL = None                        # e.g. "timestamp" (leave None to auto-detect)
OUT_PNG = "trade_count_over_time.png"
MAX_POINTS = 200_000                 # reduce if matplotlib is slow; set None to disable
# ----------------

TS_CANDIDATES = ["timestamp", "time", "ts", "created_at", "createdAt", "created", "block_timestamp"]

def infer_ts_col(cols):
    lower = {c.lower(): c for c in cols}
    for c in TS_CANDIDATES:
        if c in cols:
            return c
    for c in TS_CANDIDATES:
        if c.lower() in lower:
            return lower[c.lower()]
    return None

def ts_expr(ts_col: str) -> str:
    # Handles integer seconds/ms + ISO timestamp strings
    return f"""
    (
      CASE
        WHEN typeof({ts_col}) LIKE '%INT%' THEN
          CASE
            WHEN {ts_col} > 1000000000000 THEN to_timestamp({ts_col}/1000.0)  -- ms
            ELSE to_timestamp({ts_col})                                       -- seconds
          END
        ELSE try_cast({ts_col} AS TIMESTAMP)
      END
    )
    """.strip()

con = duckdb.connect()
parq = PARQUET_GLOB.replace("\\", "/")  # DuckDB prefers forward slashes on Windows

# get columns to auto-detect TS_COL if needed
sample = con.execute(f"SELECT * FROM read_parquet('{parq}') LIMIT 1").df()
if TS_COL is None:
    TS_COL = infer_ts_col(list(sample.columns))
if TS_COL is None:
    raise RuntimeError(f"Couldn't find a timestamp column. Columns seen: {list(sample.columns)}")

# Pull raw timestamps, order them, and compute cumulative trade count (no bucketing)
sql = f"""
WITH base AS (
  SELECT {ts_expr(TS_COL)} AS ts
  FROM read_parquet('{parq}')
),
ordered AS (
  SELECT
    ts,
    row_number() OVER (ORDER BY ts) AS cum_trades
  FROM base
  WHERE ts IS NOT NULL
)
SELECT ts, cum_trades
FROM ordered
ORDER BY ts
"""

df = con.execute(sql).df()
df["ts"] = pd.to_datetime(df["ts"], utc=True)

# optional downsample for plotting speed
if MAX_POINTS is not None and len(df) > MAX_POINTS:
    step = max(1, len(df) // MAX_POINTS)
    df = df.iloc[::step].copy()

plt.figure()
plt.plot(df["ts"], df["cum_trades"])
plt.xlabel("Timestamp (UTC)")
plt.ylabel("Cumulative number of trades")
plt.title("Total trades over time (all markets)")
plt.tight_layout()
plt.savefig(OUT_PNG, dpi=200)
plt.show()

print("Saved:", OUT_PNG)
print("Timestamp column used:", TS_COL)
print(df.head())

