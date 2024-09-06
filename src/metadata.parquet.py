import pandas as pd
import sys
import requests
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

df=pd.read_csv("https://raw.githubusercontent.com/OxCGRT/covid-policy-dataset/main/data/OxCGRT_compact_national_v1.csv")

df_meta = df[['ConfirmedDeaths', 'ConfirmedCases', 'Date']].groupby('Date').sum().reset_index()

df_meta['DailyCases'] = df_meta['ConfirmedCases'].diff().fillna(0)

# Write DataFrame to a temporary file-like object
buf = pa.BufferOutputStream()
table = pa.Table.from_pandas(df_meta)
pq.write_table(table, buf, compression="snappy")

# Get the buffer as a bytes object
buf_bytes = buf.getvalue().to_pybytes()

# Write the bytes to standard output
sys.stdout.buffer.write(buf_bytes)
