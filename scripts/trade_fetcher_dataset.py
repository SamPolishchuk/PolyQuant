import duckdb

MARKET_IDS = ["582457"]

quant_url   = "https://huggingface.co/datasets/SII-WANGZJ/Polymarket_data/resolve/main/quant.parquet"
markets_url = "https://huggingface.co/datasets/SII-WANGZJ/Polymarket_data/resolve/main/markets.parquet"

out_path = r"C:\Users\Ed\OneDrive\Desktop\trades_48h_before_close_selected_markets.csv"
out_path_sql = out_path.replace("\\", "/")

con = duckdb.connect()
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
con.execute("SET enable_progress_bar = true;")
con.execute("SET progress_bar_time = 100;")
con.execute("SET enable_progress_bar_print = true;")

con.execute("CREATE TEMP TABLE mids(mid VARCHAR)")
con.executemany("INSERT INTO mids VALUES (?)", [(m,) for m in MARKET_IDS])

trade_dt = """
COALESCE(
  TRY_CAST(q.datetime AS TIMESTAMPTZ),
  CAST(try_strptime(CAST(q.datetime AS VARCHAR), [
    '%Y-%m-%dT%H:%M:%S.%f%z',
    '%Y-%m-%d %H:%M:%S.%f%z',
    '%Y-%m-%dT%H:%M:%S%z',
    '%Y-%m-%d %H:%M:%S%z',
    '%Y-%m-%dT%H:%M:%S.%f',
    '%Y-%m-%d %H:%M:%S.%f',
    '%Y-%m-%dT%H:%M:%S',
    '%Y-%m-%d %H:%M:%S'
  ]) AS TIMESTAMPTZ)
)
"""

close_dt = """
COALESCE(
  TRY_CAST(m.end_date AS TIMESTAMPTZ),
  CAST(try_strptime(CAST(m.end_date AS VARCHAR), [
    '%Y-%m-%dT%H:%M:%S.%f%z',
    '%Y-%m-%d %H:%M:%S.%f%z',
    '%Y-%m-%dT%H:%M:%S%z',
    '%Y-%m-%d %H:%M:%S%z',
    '%Y-%m-%dT%H:%M:%S.%f',
    '%Y-%m-%d %H:%M:%S.%f',
    '%Y-%m-%dT%H:%M:%S',
    '%Y-%m-%d %H:%M:%S'
  ]) AS TIMESTAMPTZ)
)
"""

query = f"""
COPY (
  WITH mkt AS (
    SELECT
      CAST(m.id AS VARCHAR) AS mid,
      {close_dt} AS close_ts
    FROM read_parquet('{markets_url}') m
    WHERE CAST(m.id AS VARCHAR) IN (SELECT mid FROM mids)
  ),
  tr AS (
    SELECT
      q.*,
      CAST(q.market_id AS VARCHAR) AS mid,
      {trade_dt} AS dt
    FROM read_parquet('{quant_url}') q
  )
  SELECT tr.*
  FROM tr
  JOIN mkt USING (mid)
  WHERE tr.dt IS NOT NULL
    AND mkt.close_ts IS NOT NULL
    AND tr.dt >= (mkt.close_ts - INTERVAL 48 HOUR)
    AND tr.dt <= mkt.close_ts
  ORDER BY tr.dt
)
TO '{out_path_sql}' (HEADER, DELIMITER ',');
"""

con.execute(query)
print(f"Wrote {out_path}")
