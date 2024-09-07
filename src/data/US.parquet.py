import pandas as pd
import sys
import requests
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# state and national
df=pd.read_csv("https://raw.githubusercontent.com/OxCGRT/covid-policy-dataset/main/data/OxCGRT_compact_subnational_v1.csv")

df = df[df['CountryName'] == 'United States']

policy_cols = df.columns[df.columns.str.contains("^(C|E|J|V)\d", regex=True)].tolist()
sel_cols = ['RegionName', 'Date'] + policy_cols

# remove rows with missing region names
df = df.loc[~df.RegionName.isna(), sel_cols]

# convert year cat into factors
age_order = ['0-4 yrs', '5-15 yrs', '16-19 yrs', '20-24 yrs', '25-29 yrs',
             '30-34 yrs', '35-39 yrs', '40-44 yrs', '45-49 yrs', '50-54 yrs',
             '55-59 yrs', '60-64 yrs', '65-69 yrs', '70-74 yrs', '75-79 yrs',
             '80+ yrs']

df['V2B_Vaccine.age.eligibility.availability.age.floor..general.population.summary.'] = pd.Categorical(df['V2B_Vaccine.age.eligibility.availability.age.floor..general.population.summary.'], categories=age_order, ordered=True)
df['V2B_Vaccine.age.eligibility.availability.age.floor..general.population.summary.'] = df['V2B_Vaccine.age.eligibility.availability.age.floor..general.population.summary.'].cat.codes.replace(-1, np.nan)

df['V2C_Vaccine.age.eligibility.availability.age.floor..at.risk.summary.'] = pd.Categorical(df['V2C_Vaccine.age.eligibility.availability.age.floor..at.risk.summary.'], categories=age_order, ordered=True)
df['V2C_Vaccine.age.eligibility.availability.age.floor..at.risk.summary.'] = df['V2C_Vaccine.age.eligibility.availability.age.floor..at.risk.summary.'].cat.codes.replace(-1, np.nan)

# wrangle date
df['Date'] = df.Date.astype(str).map(lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:])
df['Date'] = pd.to_datetime(df['Date'])


# 1) Look at how fast and slow each policy for each state change
#    by looking at (CurrentPolicyValue - PreviousPolicyValue) / DaysBetweenChanges . 
#    When CurrentPolicy > PreviousPolicy, the SpeedOfChange will be positive.
#    When CurrentPolicy < PreviousPolicy, the SpeedOfChange will be change.
#    More days in between changes will result in a smaller SpeedOfChange.

# pivot longer
df_long = df.melt(id_vars=['RegionName', 'Date'], var_name='PolicyType', value_name='PolicyValue')

# # We will also add a column for the previous policy value
# df_long['PreviousPolicyValue'] = df_long.groupby(['RegionName', 'PolicyType'])['PolicyValue'].shift(1)

# # for now we assume that missing values are 0
# df_long['PolicyValue'] = df_long.PolicyValue.fillna(0)
# df_long['PreviousPolicyValue'] = df_long.PreviousPolicyValue.fillna(0)

# # Calculate the difference in days between the current and previous date
# df_long = df_long[df_long['PolicyValue'] != df_long['PreviousPolicyValue']]

# df_long['PreviousDate'] = df_long.groupby(['RegionName', 'PolicyType'])['Date'].shift(1)
# df_long['DaysBetweenChanges'] = (df_long['Date'] - df_long['PreviousDate']).dt.days

# df_long['SpeedOfChange'] = (df_long['PolicyValue'] - df_long['PreviousPolicyValue']) / df_long['DaysBetweenChanges']

# df_long = df_long.dropna()

# 2) Alternatively

# import matplotlib.pyplot as plt
# import seaborn as sns

# df_tmp = df[df.columns[df.columns.str.contains("C\d", regex=True)].tolist() + ["RegionName", "Date"]]
# df_tmp = df_tmp[df_tmp.columns[~df_tmp.columns.str.contains("Flag", regex=True)]]
# df_tmp.columns
# df_long = df_tmp.melt(id_vars=['RegionName', 'Date'], var_name='PolicyType', value_name='PolicyValue')
# df_long = df_long.groupby(["RegionName", "Date"])['PolicyValue'].sum().reset_index()


# sel_regions = ['Alabama', 'Arizona', 'Ohio', 'Texas', 'Pennsylvania', 'California', 'New York', 'Vermont', 'New Hampshire']
# tmp_df = df_long[df_long['RegionName'].isin(sel_regions)]

# fig, ax = plt.subplots(1, 1, figsize=(10, 5))
# sns.scatterplot(data=tmp_df, x='Date', y='PolicyValue', hue='RegionName', legend=False, ax=ax)
# sns.lineplot(data=tmp_df, x='Date', y='PolicyValue', hue='RegionName', ax=ax)

# fig, ax = plt.subplots(len(sel_regions), 1, figsize=(10, 10), sharex=True, sharey=True)
# for i, region in enumerate(sel_regions):
#     tmp_df[tmp_df['RegionName'] == region].plot(x='Date', y='PolicyValue', ax=ax[i], title=region)

# Write DataFrame to a temporary file-like object
buf = pa.BufferOutputStream()   
table = pa.Table.from_pandas(df_long)
pq.write_table(table, buf, compression="snappy")

# Get the buffer as a bytes object
buf_bytes = buf.getvalue().to_pybytes()

# Write the bytes to standard output
sys.stdout.buffer.write(buf_bytes)
