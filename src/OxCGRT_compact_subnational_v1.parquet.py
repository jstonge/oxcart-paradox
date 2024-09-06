import pandas as pd
import sys
import requests
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

df=pd.read_csv("https://raw.githubusercontent.com/OxCGRT/covid-policy-dataset/main/data/OxCGRT_compact_subnational_v1.csv")

sel_cols = ['CountryName', 'RegionName', 'Date', 'Jurisdiction'] + df.columns[df.columns.str.contains("^(C|E|J|V)\d", regex=True)].tolist()
df = df[sel_cols]

df_long = df.melt(id_vars=['CountryName', 'RegionName', 'Jurisdiction', 'Date'], var_name='PolicyType', value_name='PolicyValue')

# Write DataFrame to a temporary file-like object
buf = pa.BufferOutputStream()   
table = pa.Table.from_pandas(df_long)
pq.write_table(table, buf, compression="snappy")

# Get the buffer as a bytes object
buf_bytes = buf.getvalue().to_pybytes()

# Write the bytes to standard output
sys.stdout.buffer.write(buf_bytes)
