import pandas as pd
import pyarrow.parquet as pq


file_path = r'C:\Users\2same\Economics BSc\Quant\PolyQuant\data_probability\quant.parquet'

# # Read only the metadata/schema
# parquet_file = pq.ParquetFile(file_path)

# print("Columns and Types:")
# print(parquet_file.schema)

# print(f"\nTotal Rows in File: {parquet_file.metadata.num_rows}")

# Read just the first 10 rows using the PyArrow engine
df_head = pd.read_parquet(file_path, engine='pyarrow').head(10)

print(df_head)
print(df_head.info())