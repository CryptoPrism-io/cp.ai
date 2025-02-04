eimport time
start_time = time.time()

# @title LIBRARY
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
from sqlalchemy import create_engine
import pandas as pd
warnings.filterwarnings('ignore')

import time
start_time = time.time()

# @title  GCP/Cloud DB connect
from sqlalchemy import create_engine
import pandas as pd

# Connection parameters
db_host = "34.55.195.199"         # Public IP of your PostgreSQL instance on GCP
db_name = "dbcp"                  # Database name
db_user = "yogass09"              # Database username
db_password = "jaimaakamakhya"     # Database password
db_port = 5432                    # PostgreSQL port

# Create a SQLAlchemy engine for PostgreSQL
gcp_engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# @title SQL Query Connection to AWS for Data Listing

# Executing the query and fetching the results directly into a pandas DataFrame
with gcp_engine.connect() as connection:

    query = "SELECT * FROM crypto_listings_latest_1000"
    top_1000_cmc_rank = pd.read_sql_query(query, connection)


gcp_engine.dispose()




from sqlalchemy import create_engine
import pandas as pd

# Connection parameters
db_host = "34.55.195.199"         # Public IP of your PostgreSQL instance on GCP
db_name = "cp_ai"                  # Database name
db_user = "yogass09"              # Database username
db_password = "jaimaakamakhya"     # Database password
db_port = 5432                    # PostgreSQL port

# Create a SQLAlchemy engine for PostgreSQL
gcp_engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# @title SQL queery for con 2 for hourly data

# Executing the query and fetching the results directly into a pandas DataFrame
with gcp_engine.connect() as connection:
    query = 'SELECT * FROM "ohlcv_1h_250_coins"'
    all_coins_ohlcv_filtered = pd.read_sql_query(query, connection)


gcp_engine.dispose()


# prompt: all_coins_ohlcv_filtered.info() count unique slugs

all_coins_ohlcv_filtered.info()
print(all_coins_ohlcv_filtered['slug'].nunique())

# @title  Enhancing Function Definition Through Grouping and Indexing Techniques
df=all_coins_ohlcv_filtered
df.set_index('symbol', inplace=True)
# Ensure the timestamp column is in datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Sort the DataFrame by 'slug' and 'timestamp' columns
df.sort_values(by=['slug', 'timestamp'], inplace=True)

# Perform time-series calculations within each group (each cryptocurrency)
grouped = df.groupby('slug')

"""# TVV"""

# @title  Enhancing Function Definition Through Grouping and Indexing Techniques
df=all_coins_ohlcv_filtered
# Ensure the timestamp column is in datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Sort the DataFrame by 'slug' and 'timestamp' columns
df.sort_values(by=['slug', 'timestamp'], inplace=True)

# Perform time-series calculations within each group (each cryptocurrency)
grouped = df.groupby('slug')

#df = df.drop(df.columns[12:20], axis=1)

df.info()

# @title On-Balance Volume (OBV)

# Assuming df is your DataFrame and it is already sorted by 'slug' and 'timestamp'

def calculate_obv(group):
    # Initialize OBV list
    obv = [0]  # Start with zero for the first row
    for i in range(1, len(group)):
        if group['close'].iloc[i] > group['close'].iloc[i - 1]:
            obv.append(obv[-1] + group['volume'].iloc[i])
        elif group['close'].iloc[i] < group['close'].iloc[i - 1]:
            obv.append(obv[-1] - group['volume'].iloc[i])
        else:
            obv.append(obv[-1])
    return pd.Series(obv, index=group.index)

# Ensure the DataFrame has unique indices and reset if necessary
df = df.reset_index(drop=True)

# Group by 'slug' and apply OBV calculation
df['obv'] = df.groupby('slug').apply(calculate_obv).reset_index(level=0, drop=True)

# Recalculate the grouped DataFrame after adding the 'obv' column
grouped = df.groupby('slug')
df['m_tvv_obv_1d'] = grouped['obv'].pct_change()

df.tail()

df.describe()

# @title Moving Averages (SMA and EMA)

# Calculate the Simple Moving Average (SMA) for 9 and 18 periods
df['SMA9'] = grouped['close'].transform(lambda x: x.rolling(window=9).mean())
df['SMA18'] = grouped['close'].transform(lambda x: x.rolling(window=18).mean())

# Calculate the Exponential Moving Average (EMA) for 9 and 18 periods
df['EMA9'] = grouped['close'].transform(lambda x: x.ewm(span=9, adjust=False).mean())
df['EMA18'] = grouped['close'].transform(lambda x: x.ewm(span=18, adjust=False).mean())

# Calculate the Simple Moving Average (SMA) for 21 periods
df['SMA21'] = df.groupby('slug')['close'].transform(lambda x: x.rolling(window=21).mean())
df['SMA108'] = df.groupby('slug')['close'].transform(lambda x: x.rolling(window=108).mean())

# Calculate EMA (21-period)
df['EMA21'] = df.groupby('slug')['close'].transform(lambda x: x.ewm(span=21, adjust=False).mean())
# Calculate EMA (108-period)
df['EMA108'] = df.groupby('slug')['close'].transform(lambda x: x.ewm(span=108, adjust=False).mean())

df.info()

# @title Average True Range (ATR)
def calculate_atr(group, window=14):
    # Calculate True Range
    group['prev_close'] = group['close'].shift(1)
    group['tr1'] = group['high'] - group['low']
    group['tr2'] = abs(group['high'] - group['prev_close'])
    group['tr3'] = abs(group['low'] - group['prev_close'])
    group['TR'] = group[['tr1', 'tr2', 'tr3']].max(axis=1)

    # Calculate ATR
    group['ATR'] = group['TR'].rolling(window=window).mean()

    return group

# Apply the function to each cryptocurrency
df = df.groupby('slug').apply(calculate_atr).reset_index(level=0, drop=True)

df.info()

# @title Ketler and Donchain

def calculate_keltner_channels(group, window_ema=21, window_atr=14):
    # Calculate EMA
    group['EMA21'] = group['close'].ewm(span=window_ema, adjust=False).mean()

    # Calculate ATR
    group = calculate_atr(group, window=window_atr) # calculate_atr is now defined before being called

    # Calculate Keltner Channels
    group['Keltner_Upper'] = group['EMA21'] + (group['ATR'] * 1.5)
    group['Keltner_Lower'] = group['EMA21'] - (group['ATR'] * 1.5)

    return group

# Apply the function to each cryptocurrency
df = df.groupby('slug').apply(calculate_keltner_channels).reset_index(level=0, drop=True)

def calculate_donchian_channels(group, window=20):
    # Calculate Donchian Channels
    group['Donchian_Upper'] = group['high'].rolling(window=window).max()
    group['Donchian_Lower'] = group['low'].rolling(window=window).min()

    return group

# Reset the index before applying the function (if needed)
df = df.reset_index(drop=True) # drop=True to avoid old index being added as a column

# Apply the function to each cryptocurrency
df = df.groupby('slug').apply(calculate_donchian_channels).reset_index(level=0, drop=True)

df.info()

# @title Vwap / ADL / CMF
def calculate_vwap(group):
    # Calculate typical price for each period
    group['typical_price'] = (group['high'] + group['low'] + group['close']) / 3

    # Calculate the cumulative sum of typical price * volume
    group['cum_price_volume'] = (group['typical_price'] * group['volume']).cumsum()

    # Calculate the cumulative sum of volume
    group['cum_volume'] = group['volume'].cumsum()

    # Calculate VWAP
    group['VWAP'] = group['cum_price_volume'] / group['cum_volume']

    return group

# Reset the index before applying the function (if needed)
df = df.reset_index(drop=True) # drop=True to avoid old index being added as a column


# Group by 'slug' to calculate VWAP for each cryptocurrency
df = df.groupby('slug').apply(calculate_vwap).reset_index(level=0, drop=True)

import pandas as pd

# Correct ADL Calculation
df['ADL'] = ((df['close'] - df['low'] - (df['high'] - df['close'])) / (df['high'] - df['low'])) * df['volume']

def calculate_cmf(group, period):
    # Ensure 'slug' is not an index
    group = group.reset_index(drop=True)

    # Correct ADL Calculation
    group['ADL'] = ((group['close'] - group['low'] - (group['high'] - group['close'])) / (group['high'] - group['low'])) * group['volume']

    # Calculate cumulative ADL and volume
    group['cum_adl'] = group['ADL'].cumsum()
    group['cum_volume'] = group['volume'].cumsum()

    # Calculate CMF, handling potential division by zero
    epsilon = 1e-10  # Small constant to avoid division by zero
    group['CMF'] = group['cum_adl'].rolling(window=period).sum() / (group['cum_volume'].rolling(window=period).sum() + epsilon)

    return group

# Define the period for CMF calculation
period = 21

# Reset the index before applying the function (if needed)
df = df.reset_index(drop=True)  # drop=True to avoid old index being added as a column

# Group by 'slug' to calculate CMF for each cryptocurrency
df = df.groupby('slug').apply(calculate_cmf, period=period).reset_index(level=0, drop=True)

# prompt: create a new df called a ... take df and filter for only latest timestamp.. no need to group bby slug

# Get the latest timestamp
latest_timestamp = df['timestamp'].max()

# Filter the DataFrame for the latest timestamp
a = df[df['timestamp'] == latest_timestamp]

a.info()

# @title SQLalchemy to push (FE) data to aws db (mysql)

tvv=df

# @title Keeping Only Latest Date for Each Slug
# Group by 'slug' and get the row with the maximum timestamp
tvv = tvv.loc[tvv['timestamp'].idxmax()]
# Drop columns 4 to 10

tvv.info()

# Create a SQLAlchemy engine to connect to the MySQL database
#engine = create_engine('mysql+mysqlconnector://yogass09:jaimaakamakhya@dbcp.cry66wamma47.ap-south-1.rds.amazonaws.com:3306/dbcp')

# Write the DataFrame to a new table in the database
tvv.to_sql('FE_TVV', con=gcp_engine, if_exists='replace', index=False)

print("pct_change DataFrame uploaded to AWS MySQL database successfully!")

# @title TVV Binary Signals
columns_to_drop = ['name', 'ref_cur_id', 'ref_cur_name', 'time_open',
                   'time_close', 'time_high', 'time_low', 'open', 'high', 'low',
                   'close', 'volume', 'market_cap']

# Drop the specified columns
df_bin = df.drop(columns=columns_to_drop, errors='ignore')

df_bin['m_tvv_obv_1d_binary'] = df_bin['m_tvv_obv_1d'].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))

# prompt: SMA9, SMA18, EMA9, EMA18, SMA21, SMA108, EMA21, EMA108
# mujhe crossover calculate karne hai so 9 ka 18 ke sath and 21 ka 108 ke sath hoga jab 9 18 se jyada hai toh 1 jab 9 18 se kaam hai tab -1 same jab 21 jyada hai 108 se tab 1 and jab 21 kaam hai 108 se tab -1
# mera naming conventing hai d_tvv_sma...

# Calculate crossovers for SMA9 and SMA18
df_bin['d_tvv_sma9_18'] = (df_bin['SMA9'] > df_bin['SMA18']).astype(int) * 2 - 1

# Calculate crossovers for EMA9 and EMA18
df_bin['d_tvv_ema9_18'] = (df_bin['EMA9'] > df_bin['EMA18']).astype(int) * 2 - 1

# Calculate crossovers for SMA21 and SMA108
df_bin['d_tvv_sma21_108'] = (df_bin['SMA21'] > df_bin['SMA108']).astype(int) * 2 - 1

# Calculate crossovers for EMA21 and EMA108
df_bin['d_tvv_ema21_108'] = (df_bin['EMA21'] > df_bin['EMA108']).astype(int) * 2 - 1

# Assuming 'CMF' column exists in df_bin
threshold = 0  # Adjust this threshold as needed
# Derive bullish/bearish signals based on CMF crossing the threshold
df_bin['m_tvv_cmf'] = 0  # Initialize the new column with zeros
df_bin.loc[df_bin['CMF'] > threshold, 'm_tvv_cmf'] = 1  # Bullish signal
df_bin.loc[df_bin['CMF'] < threshold, 'm_tvv_cmf'] = -1 # Bearish signal

df_bin.info()

# @title SQLalchemy to push (FE_SIGNALS) data to aws db (mysql)

# Drop columns by their index positions
df_bin.drop(df_bin.columns[4:30], axis=1, inplace=True)
tvv_signals=df_bin

# Get the latest timestamp
latest_timestamp = df['timestamp'].max()

# Filter the DataFrame for rows where timestamp equals the latest timestamp
tvv_signals = tvv_signals[tvv_signals['timestamp'] == latest_timestamp]

# Replace infinite values with NaN
tvv_signals = tvv_signals.replace([np.inf, -np.inf], np.nan) # Replace inf values before pushing to SQL


tvv_signals.info()

# Write the DataFrame to a new table in the database
tvv_signals.to_sql('FE_TVV_SIGNALS', con=gcp_engine, if_exists='replace', index=False)

print("tvv_signals DataFrame uploaded to AWS MySQL database successfully!")

tvv_signals.info()



"""# PCT_CHANGE"""

# @title  Enhancing Function Definition Through Grouping and Indexing Techniques

df=all_coins_ohlcv_filtered
# Ensure the timestamp column is in datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Sort the DataFrame by 'slug' and 'timestamp' columns
df.sort_values(by=['slug', 'timestamp'], inplace=True)

# Perform time-series calculations within each group (each cryptocurrency)
grouped = df.groupby('slug')

df.info()

# @title  VaR & CVaR
import pandas as pd
# Calculate percentage change for each cryptocurrency
df['m_pct_1d'] = grouped['close'].pct_change()
# Calculate cumulative returns for each cryptocurrency
df['d_pct_cum_ret'] = (1 + df['m_pct_1d']).groupby(df['slug']).cumprod() - 1

# Define the confidence level, e.g., 95%
confidence_level = 0.95

# Calculate Historical VaR for each cryptocurrency
VaR_df = df.groupby('slug').apply(lambda x: x['m_pct_1d'].quantile(1 - confidence_level))
VaR_df = VaR_df.reset_index(name='d_pct_var')

# Calculate CVaR for each cryptocurrency
CVaR_df = df.groupby('slug').apply(lambda x: x['m_pct_1d'][x['m_pct_1d'] <= x['m_pct_1d'].quantile(1 - confidence_level)].mean())
CVaR_df = CVaR_df.reset_index(name='d_pct_cvar')

# Merge VaR and CVaR back into the original DataFrame
df = df.merge(VaR_df, on='slug', how='left')
df = df.merge(CVaR_df, on='slug', how='left')

df.info()

import pandas as pd
import numpy as np

# Assuming your DataFrame is named 'df'

# Ensure 'timestamp' is in datetime format and 'volume' is numeric
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['volume'] = pd.to_numeric(df['volume'])

# Sort by 'timestamp' in ascending order
df.sort_values(by='timestamp', ascending=True, inplace=True)

# Calculate daily volume percentage (VolD%)
df['d_pct_vol_1d'] = df.groupby('slug')['volume'].pct_change()

"""
# Calculate the latest weekly volume percentage (VolW%)
def latest_weekly_vol_percentage(group):
    if len(group) < 7:
        return np.nan
    return (group['volume'].iloc[-1] - group['volume'].iloc[-7]) / group['volume'].iloc[-7] * 100

df['d_pct_vol_1w'] = df.groupby('slug').apply(latest_weekly_vol_percentage).reset_index(level=0, drop=True)

# Calculate the latest monthly volume percentage (VolM%)
def latest_monthly_vol_percentage(group):
    if len(group) < 30:
        return np.nan
    return (group['volume'].iloc[-1] - group['volume'].iloc[-30]) / group['volume'].iloc[-30] * 100

df['d_pct_vol_1m'] = df.groupby('slug').apply(latest_monthly_vol_percentage).reset_index(level=0, drop=True)
"""

df.info()

# @title Keeping Only Latest Date for Each Slug
# Group by 'slug' and get the row with the maximum timestamp
pct_change = df.loc[df.groupby('slug')['timestamp'].idxmax()]

pct_change.info()

import numpy as np
# Drop columns with infinite values
pct_change = pct_change.replace([np.inf, -np.inf], np.nan)

# Drop columns 4 to 10
pct_change = pct_change.drop(pct_change.columns[4:10], axis=1)



pct_change.info()

# @title SQLalchemy to push data to aws db (mysql)

from sqlalchemy import create_engine

# Write the DataFrame to a new table in the database
pct_change.to_sql('FE_PCT_CHANGE', con=gcp_engine, if_exists='replace', index=False)

print("pct_change DataFrame uploaded to GCP postGres database successfully!")

# @title time cal and engine close

end_time = time.time()
elapsed_time_seconds = end_time - start_time
elapsed_time_minutes = elapsed_time_seconds / 60

print(f"Cell execution time: {elapsed_time_minutes:.2f} minutes")



gcp_engine.dispose()
#con.close()


"""# end of script

"""


 # Connection parameters
db_host = "34.55.195.199"         # Public IP of your PostgreSQL instance on GCP
db_name = "cp_backtest_h"                  # Database name
db_user = "yogass09"              # Database username
db_password = "jaimaakamakhya"     # Database password
db_port = 5432                    # PostgreSQL port

# Create a SQLAlchemy engine for PostgreSQL
gcp_engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Write the DataFrame to a new table in the database
tvv.to_sql('FE_TVV', con=gcp_engine, if_exists='append', index=False)
# Write the DataFrame to a new table in the database
tvv_signals.to_sql('FE_TVV_SIGNALS', con=gcp_engine, if_exists='append', index=False)

# Write the DataFrame to a new table in the database
pct_change.to_sql('FE_PCT_CHANGE', con=gcp_engine, if_exists='append', index=False)


print("table name to db name append done")

gcp_engine.dispose()

