import pandas as pd
import sys
import requests
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

URL = "https://catalog.ourworldindata.org/garden/covid/latest/cases_deaths/cases_deaths.csv"

df=pd.read_csv(URL)
df = df[['date', 'new_cases', 'country']]
# Write DataFrame to a temporary file-like object
buf = pa.BufferOutputStream()
table = pa.Table.from_pandas(df)
pq.write_table(table, buf, compression="snappy")

# Get the buffer as a bytes object
buf_bytes = buf.getvalue().to_pybytes()

# Write the bytes to standard output
sys.stdout.buffer.write(buf_bytes)
