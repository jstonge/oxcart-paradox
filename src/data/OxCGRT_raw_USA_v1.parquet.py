import pandas as pd
import sys
import requests
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# state and national
df=pd.read_csv("https://raw.githubusercontent.com/OxCGRT/covid-policy-dataset/main/data/subnat_raw/OxCGRT_raw_USA_v1.csv")


# make sure you have checked the flag variables if you see a policy that is much stricter than expected.
# because we record the strictest policy in a jurisdiction, we will sometimes 
# report a strict policy that only applies in a very small 
# geographic area (eg. a lockdown in one city when the rest of 
# the country is free). Our flag variables will usually tell you 
# if a policy is targeted to a specific geography, or general across 
# the whole jurisdiction.
df = df.loc[:, ~df.columns.str.endswith("Flag")]

# it is always useful to use the notes column to corroborate 
# the value you see in an indicator – this should explain why a 
# particular value was chosen. If there are no notes on the day 
# you are looking at, scroll back in time until the most recent note.
df = df.loc[:, ~df.columns.str.endswith("Notes")]

df_ng = df[df.Jurisdiction ==  'NAT_GOV']
df_nat = df[df.Jurisdiction ==  'NAT_TOTAL']
df_sw = df[df.Jurisdiction ==  'STATE_WIDE']

closure_policies = df_sw.columns[df_sw.columns.str.contains("^C\d", regex=True)]
economic_policies = df_sw.columns[df_sw.columns.str.contains("^E\d", regex=True)]
health_policies = df_sw.columns[df_sw.columns.str.contains("^H\d", regex=True)]
vaccination_policies = df_sw.columns[df_sw.columns.str.contains("^V\d", regex=True)].tolist()

sel_cols = ['RegionName', 'Date'] + closure_policies.tolist()

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

# sort
df_long = df_long.sort_values(by=['RegionName', 'PolicyType', 'Date'])

# We will also add a column for the previous policy value
df_long['PreviousPolicyValue'] = df_long.groupby(['RegionName', 'PolicyType'])['PolicyValue'].shift(1)

# for now we assume that missing values are 0
df_long['PolicyValue'] = df_long.PolicyValue.fillna(0)
df_long['PreviousPolicyValue'] = df_long.PreviousPolicyValue.fillna(0)

# Calculate the difference in days between the current and previous date
df_long = df_long[df_long['PolicyValue'] != df_long['PreviousPolicyValue']]

df_long['PreviousDate'] = df_long.groupby(['RegionName', 'PolicyType'])['Date'].shift(1)
df_long['DaysBetweenChanges'] = (df_long['Date'] - df_long['PreviousDate']).dt.days

df_long['SpeedOfChange'] = (df_long['PolicyValue'] - df_long['PreviousPolicyValue']) / df_long['DaysBetweenChanges']

df_long = df_long.dropna()

# 2) Alternatively

import matplotlib.pyplot as plt

df_tmp = df[df.columns[df.columns.str.contains("C\d", regex=True)].tolist() + ["RegionName", "Date"]]
df_tmp = df_tmp[df_tmp.columns[~df_tmp.columns.str.contains("Flag", regex=True)]]
df_tmp.columns
df_long = df_tmp.melt(id_vars=['RegionName', 'Date'], var_name='PolicyType', value_name='PolicyValue')
df_long = df_long.groupby(["RegionName", "Date"])['PolicyValue'].sum().reset_index()


sel_regions = ['Alabama', 'Arizona', 'Ohio', 'Texas', 'Pennsylvania', 'California', 'New York']
tmp_df = df_long[df_long['RegionName'].isin(sel_regions)]

fig, ax = plt.subplots(1, 1, figsize=(10, 5))
tmp_df.plot(x='Date', y='PolicyValue', color='RegionName', ax=ax)

fig, ax = plt.subplots(len(sel_regions), 1, figsize=(10, 10), sharex=True, sharey=True)
for i, region in enumerate(sel_regions):
    tmp_df[tmp_df['RegionName'] == region].plot(x='Date', y='PolicyValue', ax=ax[i], title=region)

# Write DataFrame to a temporary file-like object
buf = pa.BufferOutputStream()   
table = pa.Table.from_pandas(df_long)
pq.write_table(table, buf, compression="snappy")

# Get the buffer as a bytes object
buf_bytes = buf.getvalue().to_pybytes()

# Write the bytes to standard output
sys.stdout.buffer.write(buf_bytes)
