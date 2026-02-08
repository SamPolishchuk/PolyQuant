import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

quant_url = "https://huggingface.co/datasets/SII-WANGZJ/Polymarket_data/resolve/main/quant.parquet"

df_preview = con.execute(f"""
SELECT * FROM read_parquet('{quant_url}') LIMIT 5
""").fetchdf()

out_path = r"C:\Users\Ed\OneDrive\Desktop\quant_preview_5rows.csv"
df_preview.to_csv(out_path, index=False)

print(f"Wrote {out_path}")