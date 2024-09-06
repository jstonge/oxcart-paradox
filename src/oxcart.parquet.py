import pandas as pd
import sys
import requests
import pyarrow as pa
import pyarrow.parquet as pq

df_aus=pd.read_csv("https://raw.githubusercontent.com/OxCGRT/Australia-covid-policy/main/data/OxCGRT_Australia_latest.csv")

# Write DataFrame to a temporary file-like object
buf = pa.BufferOutputStream()
table = pa.Table.from_pandas(df_aus)
pq.write_table(table, buf, compression="snappy")

# Get the buffer as a bytes object
buf_bytes = buf.getvalue().to_pybytes()

# Write the bytes to standard output
sys.stdout.buffer.write(buf_bytes)
