import time
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


# @title  Enhancing Function Definition Through Grouping and Indexing Techniques  
df=all_coins_ohlcv_filtered
df.set_index('symbol', inplace=True)
# Ensure the timestamp column is in datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Sort the DataFrame by 'slug' and 'timestamp' columns
df.sort_values(by=['slug', 'timestamp'], inplace=True)

# Perform time-series calculations within each group (each cryptocurrency)
grouped = df.groupby('slug')



"""# MOMENTUM"""

# @title  Enhancing Function Definition Through Grouping and Indexing Techniques
df = all_coins_ohlcv_filtered
# Ensure the timestamp column is in datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Sort the DataFrame by 'slug' and 'timestamp' columns
df.sort_values(by=['slug', 'timestamp'], inplace=True)

# Perform time-series calculations within each group (each cryptocurrency)
grouped = df.groupby('slug')

# @title RSI 9/18/27/54/108

# Calculate percentage change for each cryptocurrency
df['m_pct_1d'] = grouped['close'].pct_change()

def calculate_rsi(df, period=1):
    # Ensure the DataFrame is sorted by timestamp
    df = df.sort_values('timestamp')

    # Calculate the difference in closing prices
    df['delta'] = df['close'].diff()

    # Calculate gains and losses
    df['gain'] = np.where(df['delta'] > 0, df['delta'], 0)
    df['loss'] = np.where(df['delta'] < 0, -df['delta'], 0)

    # Calculate average gain and average loss
    df['avg_gain'] = df['gain'].rolling(window=period, min_periods=1).mean()
    df['avg_loss'] = df['loss'].rolling(window=period, min_periods=1).mean()

    # Calculate RS (Relative Strength)
    df['rs'] = df['avg_gain'] / df['avg_loss']

    # Calculate RSI (Relative Strength Index)
    df['rsi'] = 100 - (100 / (1 + df['rs']))

    # Drop intermediate columns
    df = df.drop(columns=['delta', 'gain', 'loss', 'avg_gain', 'avg_loss', 'rs'])

    return df



# Apply the RSI calculation function to each group
df['m_mom_rsi_9'] = df.groupby('slug').apply(lambda x: calculate_rsi(x, period=9)).reset_index(level=0, drop=True)['rsi']

# Apply the RSI calculation function to each group
df['m_mom_rsi_18'] = df.groupby('slug').apply(lambda x: calculate_rsi(x, period=18)).reset_index(level=0, drop=True)['rsi']

# Apply the RSI calculation function to each group
df['m_mom_rsi_27'] = df.groupby('slug').apply(lambda x: calculate_rsi(x, period=27)).reset_index(level=0, drop=True)['rsi']

# Apply the RSI calculation function to each group
df['m_mom_rsi_54'] = df.groupby('slug').apply(lambda x: calculate_rsi(x, period=54)).reset_index(level=0, drop=True)['rsi']

# Apply the RSI calculation function to each group
df['m_mom_rsi_108'] = df.groupby('slug').apply(lambda x: calculate_rsi(x, period=108)).reset_index(level=0, drop=True)['rsi']

# @title SMA
import pandas as pd

def calculate_sma(df, column, sma_length=14):
    # Ensure the DataFrame is sorted by timestamp
    df = df.sort_values('timestamp')

    # Calculate SMA (Simple Moving Average) for the specified column
    df['sma_14'] = df[column].rolling(window=sma_length, min_periods=1).mean()

    # Normalize the SMA values between 30 and 70
    min_sma = df['sma_14'].min()
    max_sma = df['sma_14'].max()
    df['sma_14_normalized'] = 30 + (df['sma_14'] - min_sma) * (70 - 30) / (max_sma - min_sma)

    return df

# Group by 'slug' and apply the SMA calculation and normalization
df = df.groupby('slug').apply(lambda x: calculate_sma(x, 'close', sma_length=14)).reset_index(level=0, drop=True)

# @title Rate of Change (ROC)
import pandas as pd

def calculate_roc(df, period=9):
    # Ensure the DataFrame is sorted by timestamp
    df = df.sort_values('timestamp')

    # Calculate the Rate of Change (ROC)
    df['m_mom_roc'] = ((df['close'] - df['close'].shift(period)) / df['close'].shift(period)) * 100

    return df

# Apply the ROC calculation function to each group
df['m_mom_roc'] = df.groupby('slug').apply(lambda x: calculate_roc(x, period=9)).reset_index(level=0, drop=True)['m_mom_roc']

# @title Williams %R

def calculate_williams_r(df, period=14):
    # Ensure the DataFrame is sorted by timestamp
    df = df.sort_values('timestamp')

    # Calculate the Highest High and Lowest Low over the period
    df['highest_high'] = df['high'].rolling(window=period, min_periods=1).max()
    df['lowest_low'] = df['low'].rolling(window=period, min_periods=1).min()

    # Calculate Williams %R
    df['m_mom_williams_%'] = ((df['highest_high'] - df['close']) / (df['highest_high'] - df['lowest_low'])) * -100

    # Drop intermediate columns
    df = df.drop(columns=['highest_high', 'lowest_low'])

    return df

# Apply the Williams %R calculation function to each group
df['m_mom_williams_%'] = df.groupby('slug').apply(lambda x: calculate_williams_r(x, period=14)).reset_index(level=0, drop=True)['m_mom_williams_%']

# @title Stochastic Momentum Index

def calculate_smi(df, period=14, smooth_k=3, smooth_d=3):
    # Ensure the DataFrame is sorted by timestamp
    df = df.sort_values('timestamp')

    # Calculate Highest High and Lowest Low over the period
    df['highest_high'] = df['high'].rolling(window=period, min_periods=1).max()
    df['lowest_low'] = df['low'].rolling(window=period, min_periods=1).min()

    # Calculate %K
    df['percent_k'] = ((df['close'] - df['lowest_low']) / (df['highest_high'] - df['lowest_low'])) * 100

    # Smooth %K with moving average
    df['smoothed_k'] = df['percent_k'].rolling(window=smooth_k, min_periods=1).mean()

    # Calculate %D (Moving average of smoothed %K)
    df['percent_d'] = df['smoothed_k'].rolling(window=smooth_d, min_periods=1).mean()

    # Calculate SMI
    df['m_mom_smi'] = df['smoothed_k'] - df['percent_d']

    # Drop intermediate columns
    df = df.drop(columns=['highest_high', 'lowest_low', 'percent_k', 'smoothed_k', 'percent_d'])

    return df

# Apply the SMI calculation function to each group
df['m_mom_smi'] = df.groupby('slug').apply(lambda x: calculate_smi(x, period=14, smooth_k=3, smooth_d=3)).reset_index(level=0, drop=True)['m_mom_smi']

# @title Chande Momentum Oscillator

def calculate_cmo(df, period=14):
    # Ensure the DataFrame is sorted by timestamp
    df = df.sort_values('timestamp')

    # Calculate the daily price changes
    df['delta'] = df['close'].diff()

    # Calculate gains and losses
    df['gain'] = df['delta'].apply(lambda x: x if x > 0 else 0)
    df['loss'] = df['delta'].apply(lambda x: -x if x < 0 else 0)

    # Calculate the sum of gains and losses over the period
    df['sum_gain'] = df['gain'].rolling(window=period, min_periods=1).sum()
    df['sum_loss'] = df['loss'].rolling(window=period, min_periods=1).sum()

    # Calculate CMO
    df['m_mom_cmo'] = (df['sum_gain'] - df['sum_loss']) / (df['sum_gain'] + df['sum_loss']) * 100

    # Drop intermediate columns
    df = df.drop(columns=['delta', 'gain', 'loss', 'sum_gain', 'sum_loss'])

    return df

# Apply the CMO calculation function to each group
df['m_mom_cmo'] = df.groupby('slug').apply(lambda x: calculate_cmo(x, period=14)).reset_index(level=0, drop=True)['m_mom_cmo']

# @title Momentum (MOM)

def calculate_mom(df, period=10):
    # Ensure the DataFrame is sorted by timestamp
    df = df.sort_values('timestamp')

    # Calculate Momentum
    df['m_mom_mom'] = df['close'] - df['close'].shift(period)

    return df

# Apply the MOM calculation function to each group
df['m_mom_mom'] = df.groupby('slug').apply(lambda x: calculate_mom(x, period=10)).reset_index(level=0, drop=True)['m_mom_mom']

# @title True Strength Index

def calculate_tsi(df, short_period=13, long_period=25):
    # Ensure the DataFrame is sorted by timestamp
    df = df.sort_values('timestamp')

    # Calculate the price changes
    df['delta'] = df['close'].diff()

    # Calculate the smoothed price changes using exponential smoothing
    df['smoothed_delta_short'] = df['delta'].ewm(span=short_period, adjust=False).mean()
    df['smoothed_delta_long'] = df['delta'].ewm(span=long_period, adjust=False).mean()

    # Calculate the TSI
    df['m_mom_tsi'] = 100 * (df['smoothed_delta_short'].ewm(span=short_period, adjust=False).mean() /
                       df['smoothed_delta_long'].ewm(span=long_period, adjust=False).mean())

    # Drop intermediate columns
    df = df.drop(columns=['delta', 'smoothed_delta_short', 'smoothed_delta_long'])

    return df

# Apply the TSI calculation function to each group
df['m_mom_tsi'] = df.groupby('slug').apply(lambda x: calculate_tsi(x, short_period=13, long_period=25)).reset_index(level=0, drop=True)['m_mom_tsi']

# @title Shortening the DF for Easier Computation
# Ensure 'timestamp' column is in datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Calculate the date 108 days ago from the most recent date
start_date = df['timestamp'].max() - pd.Timedelta(days=32)

# Filter the DataFrame for the last 108 days
df = df[df['timestamp'] >= start_date]

# @title SQLalchemy to push (FE_SIGNALS) data to aws db (mysql)

# @title SQLalchemy to push (FE) data to aws db (mysql)
COLUMNS_TO_KEEP_MOMENTUM = [
    'id', 'slug', 'name', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'market_cap', 
    'm_pct_1d', 'm_mom_rsi_9', 'm_mom_rsi_18', 'm_mom_rsi_27', 'm_mom_rsi_54', 'm_mom_rsi_108', 
    'sma_14', 'sma_14_normalized', 'm_mom_roc', 'm_mom_williams_%', 'm_mom_smi', 'm_mom_cmo', 
    'm_mom_mom', 'm_mom_tsi'
]

df=df[COLUMNS_TO_KEEP_MOMENTUM]
momentum=df

# Get the latest timestamp
latest_timestamp = momentum['timestamp'].max()

# Filter the DataFrame for rows where timestamp equals the latest timestamp
momentum = momentum[momentum['timestamp'] == latest_timestamp]

# Replace infinite values with NaN
momentum = momentum.replace([np.inf, -np.inf], np.nan) # Replace inf values before pushing to SQL

# Create a SQLAlchemy engine to connect to the MySQL database
#engine = create_engine('mysql+mysqlconnector://yogass09:jaimaakamakhya@dbcp.cry66wamma47.ap-south-1.rds.amazonaws.com:3306/dbcp')

# Write the DataFrame to a new table in the database
momentum.to_sql('FE_MOMENTUM', con=gcp_engine, if_exists='replace', index=False)

print("momentum DataFrame uploaded to AWS MySQL database successfully!")

momentum.info()
momentum_df=momentum

# @title Momentum Binary Signals

def map_roc_values(row):
    # Map ROC values to 1 if greater than 0, -1 if less than 0, else 0
    if row['m_mom_roc'] > 0:
        return 1
    elif row['m_mom_roc'] < 0:
        return -1
    else:
        return 0

# Apply the mapping function to each row
df['m_mom_roc_bin'] = df.apply(map_roc_values, axis=1)

import pandas as pd

def map_williams_r(row):
    # Map Williams %R values based on the specified conditions
    if row['m_mom_williams_%'] > -50:
        return 1
    elif row['m_mom_williams_%'] < -50:
        return -1
    else:
        return 0

# Apply the mapping function to each row
df['m_mom_williams_%_bin'] = df.apply(map_williams_r, axis=1)

import pandas as pd

# Define a function to map SMI values
def map_smi(value):
    if value >= 25:
        return 1
    elif value <= -25:
        return -1
    else:
        return 0

# Apply the mapping function to 'm_mom_smi' column
df['m_mom_smi_bin'] = df['m_mom_smi'].apply(map_smi)

import pandas as pd

# Define a function to map CMO values
def map_cmo(value):
    if value > 40:
        return 1
    elif value < -40:
        return -1
    else:
        return 0

# Apply the mapping function to 'm_mom_cmo' column
df['m_mom_cmo_bin'] = df['m_mom_cmo'].apply(map_cmo)

import pandas as pd

# Define a function to map Momentum values
def map_momentum(value):
    if value > 4000:
        return 1
    elif value < -4000:
        return -1
    else:
        return 0

# Apply the mapping function to 'm_mom_mom' column
df['m_mom_mom_bin'] = df['m_mom_mom'].apply(map_momentum)

import pandas as pd



df_momentum = df

df_momentum.info()

# @title SQLalchemy to push (FE_SIGNALS) data to aws db (mysql)
COLOUMS_TO_KEEP= ['id', 'slug', 'name', 'timestamp', 
    'm_mom_roc_bin', 'm_mom_williams_%_bin', 'm_mom_smi_bin', 
    'm_mom_cmo_bin', 'm_mom_mom_bin']


df_momentum=df_momentum[COLOUMS_TO_KEEP]


# Get the latest timestamp
latest_timestamp = df_momentum['timestamp'].max()

# Filter the DataFrame for rows where timestamp equals the latest timestamp
df_momentum = df_momentum[df_momentum['timestamp'] == latest_timestamp]

# Replace infinite values with NaN
df_momentum = df_momentum.replace([np.inf, -np.inf], np.nan) # Replace inf values before pushing to SQL

# Create a SQLAlchemy engine to connect to the MySQL database
#engine = create_engine('mysql+mysqlconnector://yogass09:jaimaakamakhya@dbcp.cry66wamma47.ap-south-1.rds.amazonaws.com:3306/dbcp')

# Write the DataFrame to a new table in the database
df_momentum.to_sql('FE_MOMENTUM_SIGNALS', con=gcp_engine, if_exists='replace', index=False)

print("FE_MOMENTUM_SIGNALS DataFrame uploaded to AWS MySQL database successfully!")

df_momentum.info()

"""# OSCILLATOR"""

# @title  Enhancing Function Definition Through Grouping and Indexing Techniques
df = all_coins_ohlcv_filtered
# Ensure the timestamp column is in datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Sort the DataFrame by 'slug' and 'timestamp' columns
df.sort_values(by=['slug', 'timestamp'], inplace=True)

# Perform time-series calculations within each group (each cryptocurrency)
grouped = df.groupby('slug')
# Calculate percentage change for each cryptocurrency
df['m_pct_1d'] = grouped['close'].pct_change()

# Calculate cumulative returns for each cryptocurrency
df['d_pct_cum_ret'] = (1 + df['m_pct_1d']).groupby(df['slug']).cumprod() - 1

# @title MACD (Moving Average Convergence Divergence)
# Define the function to calculate MACD
def calculate_macd(group):
    # Calculate the 12-day EMA
    group['EMA_12'] = group['close'].ewm(span=12, adjust=False).mean()

    # Calculate the 26-day EMA
    group['EMA_26'] = group['close'].ewm(span=26, adjust=False).mean()

    # Calculate the MACD line
    group['MACD'] = group['EMA_12'] - group['EMA_26']

    # Calculate the Signal line (9-day EMA of the MACD line)
    group['Signal'] = group['MACD'].ewm(span=9, adjust=False).mean()

    return group

# Apply this function to each group (coin)
df = df.groupby('slug').apply(calculate_macd).reset_index(level=0, drop=True)

# @title CCI (Commodity Channel Index)
def calculate_cci(group, period=20):
    # Calculate Typical Price
    group['TP'] = (group['high'] + group['low'] + group['close']) / 3

    # Calculate SMA of Typical Price
    group['SMA_TP'] = group['TP'].rolling(window=period).mean()

    # Calculate Mean Absolute Deviation manually
    def mean_absolute_deviation(series):
        return (series - series.mean()).abs().mean()

    group['MAD'] = group['TP'].rolling(window=period).apply(mean_absolute_deviation, raw=False)

    # Calculate CCI
    group['CCI'] = (group['TP'] - group['SMA_TP']) / (0.015 * group['MAD'])

    return group

df = df.groupby('slug').apply(calculate_cci).reset_index(level=0, drop=True)

# @title ADX (Average Directional Index)

def calculate_adx(group, period=14):
    # Ensure 'timestamp' is sorted
    group = group.sort_values('timestamp')

    # Calculate True Range (TR)
    group['TR'] = pd.concat([
        group['high'] - group['low'],
        (group['high'] - group['close'].shift()).abs(),
        (group['low'] - group['close'].shift()).abs()
    ], axis=1).max(axis=1)

    # Calculate Directional Movement (+DM and -DM)
    group['+DM'] = ((group['high'] - group['high'].shift()) > (group['low'].shift() - group['low'])) & (group['high'] - group['high'].shift() > 0) * (group['high'] - group['high'].shift())
    group['-DM'] = ((group['low'].shift() - group['low']) > (group['high'] - group['high'].shift())) & (group['low'].shift() - group['low'] > 0) * (group['low'].shift() - group['low'])

    # Calculate Smoothed Averages for +DM, -DM, and TR
    group['Smoothed_TR'] = group['TR'].rolling(window=period).sum()
    group['Smoothed_+DM'] = group['+DM'].rolling(window=period).sum()
    group['Smoothed_-DM'] = group['-DM'].rolling(window=period).sum()

    # Calculate +DI and -DI
    group['+DI'] = 100 * group['Smoothed_+DM'] / group['Smoothed_TR']
    group['-DI'] = 100 * group['Smoothed_-DM'] / group['Smoothed_TR']

    # Calculate DX
    group['DX'] = 100 * abs(group['+DI'] - group['-DI']) / (group['+DI'] + group['-DI'])

    # Calculate ADX
    group['ADX'] = group['DX'].rolling(window=period).mean()

    return group

# Apply the ADX calculation function to each cryptocurrency
df = df.groupby('slug').apply(calculate_adx).reset_index(level=0, drop=True)

# @title Ultimate Oscillator (UO)
def calculate_ultimate_oscillator(group, short_period=3, intermediate_period=6, long_period=9):
    # Ensure 'timestamp' is sorted
    group = group.sort_values('timestamp')

    # Calculate True Range (TR)
    group['prev_close'] = group['close'].shift(1)
    group['TR'] = pd.concat([
        group['high'] - group['low'],
        (group['high'] - group['prev_close']).abs(),
        (group['low'] - group['prev_close']).abs()
    ], axis=1).max(axis=1)

    # Calculate Buying Pressure (BP)
    group['BP'] = group['close'] - group[['low', 'prev_close']].min(axis=1)

    # Calculate Smoothed BP and TR for different periods
    group['Avg_BP_short'] = group['BP'].rolling(window=short_period).sum()
    group['Avg_TR_short'] = group['TR'].rolling(window=short_period).sum()

    group['Avg_BP_intermediate'] = group['BP'].rolling(window=intermediate_period).sum()
    group['Avg_TR_intermediate'] = group['TR'].rolling(window=intermediate_period).sum()

    group['Avg_BP_long'] = group['BP'].rolling(window=long_period).sum()
    group['Avg_TR_long'] = group['TR'].rolling(window=long_period).sum()

    # Calculate Ultimate Oscillator (UO)
    group['UO'] = 100 * (
        (4 * group['Avg_BP_short'] + 2 * group['Avg_BP_intermediate'] + group['Avg_BP_long']) /
        (4 * group['Avg_TR_short'] + 2 * group['Avg_TR_intermediate'] + group['Avg_TR_long'])
    )

    return group

# Apply the Ultimate Oscillator calculation function to each cryptocurrency
df = df.groupby('slug').apply(calculate_ultimate_oscillator).reset_index(level=0, drop=True)

# @title Awesome Oscillator (AO)

def calculate_awesome_oscillator(group):
    # Ensure 'timestamp' is sorted
    group = group.sort_values('timestamp')

    # Calculate Median Price (MP)
    group['MP'] = (group['high'] + group['low']) / 2

    # Calculate the 5-period and 34-period SMA of the Median Price
    group['SMA_5'] = group['MP'].rolling(window=5).mean()
    group['SMA_34'] = group['MP'].rolling(window=34).mean()

    # df['SMA_5'] = grouped['close'].transform(lambda x: x.rolling(window=9).mean())

    # Calculate the Awesome Oscillator (AO)
    group['AO'] = group['SMA_5'] - group['SMA_34']

    return group

# Apply the Awesome Oscillator calculation function to each cryptocurrency
df = df.groupby('slug').apply(calculate_awesome_oscillator).reset_index(level=0, drop=True)

# @title TRIX (Triple Exponential Average)

def calculate_trix(group, period=15):
    # Ensure 'timestamp' is sorted
    group = group.sort_values('timestamp')

    # Calculate the Triple Exponential Moving Average (TEMA)
    group['EMA1'] = group['close'].ewm(span=period, adjust=False).mean()
    group['EMA2'] = group['EMA1'].ewm(span=period, adjust=False).mean()
    group['EMA3'] = group['EMA2'].ewm(span=period, adjust=False).mean()

    # Calculate TRIX Oscillator
    group['TRIX'] = group['EMA3'].pct_change() * 100

    return group

# Apply the TRIX Oscillator calculation function to each cryptocurrency
df = df.groupby('slug').apply(calculate_trix).reset_index(level=0, drop=True)

df.info(0)



oscillator=df

# Get the latest timestamp
latest_timestamp = df['timestamp'].max()

# Filter the DataFrame for rows where timestamp equals the latest timestamp
df = df[df['timestamp'] == latest_timestamp]

# Replace infinite values with NaN
oscillator = df.replace([np.inf, -np.inf], np.nan) # Replace inf values before pushing to SQL

# Create a SQLAlchemy engine to connect to the MySQL database
#engine = create_engine('mysql+mysqlconnector://yogass09:jaimaakamakhya@dbcp.cry66wamma47.ap-south-1.rds.amazonaws.com:3306/dbcp')

# Write the DataFrame to a new table in the database
oscillator.to_sql('FE_OSCILLATOR', con=gcp_engine, if_exists='replace', index=False)

print("oscillator DataFrame uploaded to AWS MySQL database successfully!")

oscillator.info()

# @title Oscillator Binanry Signals
# Drop the specified columns
#df_bin = df.drop(columns=columns_to_drop, errors='ignore')
df_bin=oscillator
# prompt: can you help me cal ... where macd is greater than signal ... mark it as 1 in a col name called macd_crossover and vice a vesa for -1

# Create a new column 'macd_crossover' and initialize it with 0
df_bin['m_osc_macd_crossover_bin'] = 0

# Set 'macd_crossover' to 1 where MACD is greater than Signal
df_bin.loc[df_bin['MACD'] > df_bin['Signal'], 'm_osc_macd_crossover_bin'] = 1

# Set 'macd_crossover' to -1 where MACD is less than Signal
df_bin.loc[df_bin['MACD'] < df_bin['Signal'], 'm_osc_macd_crossover_bin'] = -1

# prompt: can you help me cal ... where cci is greater than 200 1 and and where it is less than 200 -1 and between that 0 neutral

# Create a new column 'cci_signal' and initialize it with 0
df_bin['m_osc_cci_bin'] = 0

# Set 'cci_signal' to 1 where CCI is greater than 200
df_bin.loc[df_bin['CCI'] > 108, 'm_osc_cci_bin'] = 1

# Set 'cci_signal' to -1 where CCI is less than -200
df_bin.loc[df_bin['CCI'] < -108, 'm_osc_cci_bin'] = -1

# prompt: can you help me cal ... where +DI is greater than -DI and adx is greater or equals to 20 then 1 ... and when  where -DI is greater than -DI and adx is greater or equals to 20 then -1 else 0
#  23  ADX

# Create a new column 'adx_signal' and initialize it with 0
df_bin['m_osc_adx_bin'] = 0

# Set 'adx_signal' to 1 where +DI is greater than -DI and ADX is greater than or equal to 20
df_bin.loc[(df_bin['+DI'] > df_bin['-DI']) & (df_bin['ADX'] >= 20), 'm_osc_adx_bin'] = 1

# Set 'adx_signal' to -1 where -DI is greater than +DI and ADX is greater than or equal to 20
df_bin.loc[(df_bin['-DI'] > df_bin['+DI']) & (df_bin['ADX'] >= 20), 'm_osc_adx_bin'] = -1

# prompt: can you help me cal ... where uo is less than 33 = 1 and uo is more then 67 = -1 or else 0

# Create a new column 'uo_signal' and initialize it with 0
df_bin['m_osc_uo_bin'] = 0

# Set 'uo_signal' to 1 where UO is less than 33
df_bin.loc[df_bin['UO'] < 33, 'm_osc_uo_bin'] = 1

# Set 'uo_signal' to -1 where UO is greater than 67
df_bin.loc[df_bin['UO'] > 67, 'm_osc_uo_bin'] = -1


# Create a new column 'ao_signal' and initialize it with 0
df_bin['m_osc_ao_bin'] = 0

# Set 'ao_signal' to 1 where AO is greater than 0
df_bin.loc[df_bin['AO'] > 0, 'm_osc_ao_bin'] = 1

# Set 'ao_signal' to -1 where AO is less than 0
df_bin.loc[df_bin['AO'] < 0, 'm_osc_ao_bin'] = -1

# prompt: do the same logic as AO just for TRIX

# Create a new column 'trix_signal' and initialize it with 0
df_bin['m_osc_trix_bin'] = 0

# Set 'trix_signal' to 1 where TRIX is greater than 0
df_bin.loc[df_bin['TRIX'] > 0, 'm_osc_trix_bin'] = 1

# Set 'trix_signal' to -1 where TRIX is less than 0
df_bin.loc[df_bin['TRIX'] < 0, 'm_osc_trix_bin'] = -1

# Convert 'timestamp' to datetime if it's not already
df_bin['timestamp'] = pd.to_datetime(df_bin['timestamp'])

# Find the latest date in the 'timestamp' column
latest_date = df_bin['timestamp'].dt.date.max()

# Filter the DataFrame for the latest date
df_filtered = df_bin[df_bin['timestamp'].dt.date == latest_date]

df_oscillator_bin = df_filtered

df_oscillator_bin.info()

df_oscillator_bin.head()

# @title SQLalchemy to push (FE_SIGNALS) data to aws db (mysql)

# Drop columns by their index positions
df_oscillator_bin.drop(df_oscillator_bin.columns[4:46], axis=1, inplace=True)


# Get the latest timestamp
latest_timestamp = df_oscillator_bin['timestamp'].max()

# Filter the DataFrame for rows where timestamp equals the latest timestamp
df_oscillator_bin = df_oscillator_bin[df_oscillator_bin['timestamp'] == latest_timestamp]

# Replace infinite values with NaN
df_oscillator_bin = df_oscillator_bin.replace([np.inf, -np.inf], np.nan) # Replace inf values before pushing to SQL

# Create a SQLAlchemy engine to connect to the MySQL database
#engine = create_engine('mysql+mysqlconnector://yogass09:jaimaakamakhya@dbcp.cry66wamma47.ap-south-1.rds.amazonaws.com:3306/dbcp')

# Write the DataFrame to a new table in the database
df_oscillator_bin.to_sql('FE_OSCILLATORS_SIGNALS', con=gcp_engine, if_exists='replace', index=False)

print("FE_OSCILLATORS_SIGNALS DataFrame uploaded to AWS MySQL database successfully!")

df_oscillator_bin.info()



"""# RATIOS"""

# @title  Enhancing Function Definition Through Grouping and Indexing Techniques
df = all_coins_ohlcv_filtered

# Ensure the timestamp column is in datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Determine the end date as the most recent date in the dataset
end_date = df['timestamp'].max()

# Calculate the start date as 30 days before the end date
start_date = end_date - pd.Timedelta(days=30)

# Filter the DataFrame to include only the last 120 days
df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]

# @title Benchmark
# Ensure the timestamp column is in datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Sort the DataFrame by 'slug' and 'timestamp' columns
df.sort_values(by=['slug', 'timestamp'], inplace=True)

# Perform time-series calculations within each group (each cryptocurrency)
grouped = df.groupby('slug')

# Separate dataframes for bitcoin (benchmark) and other cryptocurrencies
benchmark_df = df[df['slug'] == 'bitcoin']
crypto_df = df[df['slug'] != 'bitcoin']

# Calculate average returns for bitcoin (benchmark)
benchmark_avg_return = benchmark_df['m_pct_1d'].mean()

# @title Alpha
def calculate_alpha(group):
    avg_return = group['m_pct_1d'].mean()
    excess_return = avg_return - benchmark_avg_return
    return pd.Series({'m_rat_alpha': excess_return})

# Apply the alpha calculation for each cryptocurrency
alpha_df = crypto_df.groupby('slug').apply(calculate_alpha).reset_index()

# @title Benchmark return dependent variable after alpha
# Ensure the timestamp column is in datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Sort the DataFrame by 'slug' and 'timestamp' columns
df.sort_values(by=['slug', 'timestamp'], inplace=True)

# Separate dataframes for bitcoin (benchmark) and other cryptocurrencies
benchmark_df = df[df['slug'] == 'bitcoin']
crypto_df = df[df['slug'] != 'bitcoin']

# Calculate benchmark returns for beta calculation
benchmark_returns = benchmark_df.set_index('timestamp')['m_pct_1d']

# @title Beta
def calculate_beta(group):
    group = group.set_index('timestamp')
    covariance = group['m_pct_1d'].cov(benchmark_returns)
    benchmark_variance = benchmark_returns.var()
    beta = covariance / benchmark_variance
    return pd.Series({'d_rat_beta': beta})

# Apply the calculation for each cryptocurrency
beta_df = crypto_df.groupby('slug').apply(calculate_beta).reset_index()

# @title Omega
def calculate_omega_ratio(group):
    group = group.set_index('timestamp')
    # Remove or aggregate duplicates
    group = group[~group.index.duplicated(keep='first')]  # or use a method to aggregate duplicates if needed

    returns = group['m_pct_1d']
    # Calculate excess returns over the benchmark
    # Ensure no duplicates in benchmark_returns before reindexing
    benchmark_returns_unique = benchmark_returns[~benchmark_returns.index.duplicated(keep='first')]
    excess_returns = returns - benchmark_returns_unique.reindex(returns.index, method='ffill')

    # Calculate average gains and losses
    average_gain = excess_returns[excess_returns > 0].mean()
    average_loss = abs(excess_returns[excess_returns < 0].mean())

    # Omega ratio calculation
    omega_ratio = average_gain / average_loss if average_loss != 0 else float('inf')

    return pd.Series({'m_rat_omega': omega_ratio})

    # Apply the calculation for each cryptocurrency
omega_df = crypto_df.groupby('slug').apply(calculate_omega_ratio).reset_index()

# @title Sharpe & Sortino

# Define risk-free rate (example: 0.02 for 2%)
risk_free_rate = 0
def calculate_sharpe_ratio(group):
    group = group.set_index('timestamp')
    returns = group['m_pct_1d']
    # Calculate excess returns over the risk-free rate
    excess_returns = returns - risk_free_rate

    # Calculate Sharpe ratio
    average_return = excess_returns.mean()
    std_dev = excess_returns.std()
    sharpe_ratio = average_return / std_dev

    return pd.Series({'v_rat_sharpe': sharpe_ratio})


def calculate_sortino_ratio(group):
    group = group.set_index('timestamp')
    returns = group['m_pct_1d']
    # Calculate excess returns over the risk-free rate
    excess_returns = returns - risk_free_rate

    # Calculate downside deviation
    downside_returns = excess_returns[excess_returns < 0]
    downside_deviation = downside_returns.std()

    # Calculate Sortino ratio
    average_return = excess_returns.mean()
    sortino_ratio = average_return / downside_deviation

    return pd.Series({'v_rat_sortino': sortino_ratio})

# Apply the calculation for each cryptocurrency
sharpe_df = crypto_df.groupby('slug').apply(calculate_sharpe_ratio).reset_index()
sortino_df = crypto_df.groupby('slug').apply(calculate_sortino_ratio).reset_index()

# @title Teynor
def calculate_treynor_ratio(group):
    group = group.set_index('timestamp')
    returns = group['m_pct_1d']
    # Calculate average return
    average_return = returns.mean()

    # Get beta for the current slug
    slug = group['slug'].iloc[0]
    # Use the correct column name 'd_rat_beta'
    beta = beta_df.loc[beta_df['slug'] == slug, 'd_rat_beta'].values[0]

    # Calculate Treynor ratio
    treynor_ratio = (average_return - risk_free_rate) / beta if beta != 0 else float('inf')

    return pd.Series({'v_rat_teynor': treynor_ratio})

treynor_df = crypto_df.groupby('slug').apply(calculate_treynor_ratio).reset_index()

# @title CommonSense Ratio

def calculate_common_sense_ratio(group):
    group = group.set_index('timestamp')
    returns = group['m_pct_1d']

    # Calculate mean return
    mean_return = returns.mean()

    # Calculate maximum drawdown
    cumulative_returns = (1 + returns).cumprod()
    peak = cumulative_returns.cummax()
    drawdown = (cumulative_returns - peak) / peak
    max_drawdown = drawdown.min()

    # Calculate Common Sense Ratio
    common_sense_ratio = mean_return / abs(max_drawdown) if max_drawdown != 0 else float('inf')

    return pd.Series({'v_rat_common_sense': common_sense_ratio})

# Apply the calculation for each cryptocurrency
common_sense_df = crypto_df.groupby('slug').apply(calculate_common_sense_ratio).reset_index()

# @title Information Ratio
def calculate_information_ratio(group):
    group = group.set_index('timestamp')
    returns = group['m_pct_1d']
    # Calculate active returns over the benchmark
    # Ensure no duplicates in benchmark_returns before reindexing
    benchmark_returns_unique = benchmark_returns[~benchmark_returns.index.duplicated(keep='first')]
    active_returns = returns - benchmark_returns_unique.reindex(returns.index, method='ffill')

    # Calculate mean of active returns (Active Return)
    active_return = active_returns.mean()

    # Calculate standard deviation of active returns (Tracking Error)
    tracking_error = active_returns.std()

    # Calculate Information Ratio
    information_ratio = active_return / tracking_error if tracking_error != 0 else float('inf')

    return pd.Series({'v_rat_information': information_ratio})

# Apply the calculation for each cryptocurrency
information_ratio_df = crypto_df.groupby('slug').apply(calculate_information_ratio).reset_index()

# @title WinLossRatio
def calculate_winloss_ratio(group):
    group = group.set_index('timestamp')
    returns = group['m_pct_1d']

    # Calculate number of winning and losing trades
    num_wins = (returns > 0).sum()
    num_losses = (returns < 0).sum()

    # Calculate Win/Loss Ratio
    winloss_ratio = num_wins / num_losses if num_losses != 0 else float('inf')

    return pd.Series({'v_rat_win_loss': winloss_ratio})

# Apply the calculation for each cryptocurrency
winloss_df = crypto_df.groupby('slug').apply(calculate_winloss_ratio).reset_index()

# @title WinRate
def calculate_win_rate(group):
    group = group.set_index('timestamp')
    returns = group['m_pct_1d']

    # Calculate number of winning trades and total trades
    num_wins = (returns > 0).sum()
    total_trades = len(returns)

    # Calculate Win Rate
    win_rate = num_wins / total_trades if total_trades != 0 else float('inf')

    return pd.Series({'m_rat_win_rate': win_rate})

# Apply the calculation for each cryptocurrency
win_rate_df = crypto_df.groupby('slug').apply(calculate_win_rate).reset_index()

# @title Risk of Ruin

# Define function to calculate Win Rate
def calculate_win_rate_ror(group):
    returns = group['m_pct_1d']

    # Calculate number of winning trades and total trades
    num_wins = (returns > 0).sum()
    total_trades = len(returns)

    # Calculate Win Rate
    win_rate = num_wins / total_trades if total_trades != 0 else float('inf')

    return win_rate, total_trades

def calculate_risk_of_ruin(group):
    win_rate, total_trades = calculate_win_rate_ror(group)

    # Calculate Risk of Ruin
    if win_rate == float('inf'):
        risk_of_ruin = 0
    else:
        risk_of_ruin = ((1 - win_rate) / (win_rate)) ** total_trades

    return pd.Series({'m_rat_ror': risk_of_ruin})

# Apply the calculation for each cryptocurrency
risk_of_ruin_df = crypto_df.groupby('slug').apply(calculate_risk_of_ruin).reset_index()

# @title Gain to Pain
def calculate_gain_to_pain(group):
    group = group.set_index('timestamp')
    returns = group['m_pct_1d']

    # Calculate Total Gain and Total Pain
    total_gain = returns[returns > 0].sum()
    total_pain = abs(returns[returns < 0]).sum()

    # Calculate Gain-to-Pain Ratio
    gain_to_pain_ratio = total_gain / total_pain if total_pain != 0 else float('inf')

    return pd.Series({'d_rat_pain': gain_to_pain_ratio})

# Apply the calculation for each cryptocurrency
gain_to_pain_df = crypto_df.groupby('slug').apply(calculate_gain_to_pain).reset_index()

# @title Joining All Ratios in a single DF
# List of DataFrames
dfs = [
    alpha_df,
    beta_df,
    omega_df,
    sharpe_df,
    sortino_df,
    treynor_df,
    common_sense_df,
    information_ratio_df,
    winloss_df,
    win_rate_df,
    risk_of_ruin_df,
    gain_to_pain_df
]

# Perform the inner join on 'slug'
def inner_join_on_slug(dfs):
    # Start with the first DataFrame
    merged_df = dfs[0]

    # Iteratively merge with the rest of the DataFrames
    for df in dfs[1:]:
        merged_df = pd.merge(merged_df, df, on='slug', how='inner')

    return merged_df

# Perform the join
merged_df = inner_join_on_slug(dfs)
ratios = merged_df

df_oscillator_bin.info()

# prompt: in ratios add timestamp from df_oscillator_bin

import pandas as pd
# Merge ratios and df_oscillator_bin on 'slug' to get the latest timestamp
ratios = pd.merge(ratios, df_oscillator_bin[['slug', 'id', 'name','timestamp']], on='slug', how='left')

# @title SQLalchemy to push (FE) data to aws db (mysql)


# Replace infinite values with NaN
ratios = ratios.replace([np.inf, -np.inf], np.nan) # Replace inf values before pushing to SQL

# Create a SQLAlchemy engine to connect to the MySQL database
#engine = create_engine('mysql+mysqlconnector://yogass09:jaimaakamakhya@dbcp.cry66wamma47.ap-south-1.rds.amazonaws.com:3306/dbcp')

# Write the DataFrame to a new table in the database
ratios.to_sql('FE_RATIOS', con=gcp_engine, if_exists='replace', index=False)
ratios_df=ratios
print("FE_RATIOS DataFrame uploaded to AWS MySQL database successfully!")

ratios.info()

# @title Ratios Binanry Signals
# Create a new column 'alpha_signal' and initialize it with 0
ratios['m_rat_alpha_bin'] = 0
ratios.loc[ratios['m_rat_alpha'] > 0, 'm_rat_alpha_bin'] = 1
ratios.loc[ratios['m_rat_alpha'] < 0, 'm_rat_alpha_bin'] = -1

# Repeat for beta, Sharpe, Sortino, Treynor, and Common Sense
ratios['d_rat_beta_bin'] = 0
ratios.loc[ratios['d_rat_beta'] > 0, 'd_rat_beta_bin'] = 1
ratios.loc[ratios['d_rat_beta'] < 0, 'd_rat_beta_bin'] = -1

ratios['v_rat_sharpe_bin'] = 0
ratios.loc[ratios['v_rat_sharpe'] > 0, 'v_rat_sharpe_bin'] = 1
ratios.loc[ratios['v_rat_sharpe'] < 0, 'v_rat_sharpe_bin'] = -1

ratios['v_rat_sortino_bin'] = 0
ratios.loc[ratios['v_rat_sortino'] > 0, 'v_rat_sortino_bin'] = 1
ratios.loc[ratios['v_rat_sortino'] < 0, 'v_rat_sortino_bin'] = -1

ratios['v_rat_teynor_bin'] = 0
ratios.loc[ratios['v_rat_teynor'] > 0, 'v_rat_teynor_bin'] = 1
ratios.loc[ratios['v_rat_teynor'] < 0, 'v_rat_teynor_bin'] = -1

ratios['v_rat_common_sense_bin'] = 0
ratios.loc[ratios['v_rat_common_sense'] > 0, 'v_rat_common_sense_bin'] = 1
ratios.loc[ratios['v_rat_common_sense'] < 0, 'v_rat_common_sense_bin'] = -1

# v_rat_information
ratios['v_rat_information_bin'] = 0
ratios.loc[ratios['v_rat_information'] < -0.1, 'v_rat_information_bin'] = -1
ratios.loc[ratios['v_rat_information'] >= -0.1, 'v_rat_information_bin'] = 1

# v_rat_win_loss
ratios['v_rat_win_loss_bin'] = 0
ratios.loc[ratios['v_rat_win_loss'] < 1, 'v_rat_win_loss_bin'] = -1
ratios.loc[ratios['v_rat_win_loss'] >= 1, 'v_rat_win_loss_bin'] = 1

# m_rat_win_rate
ratios['m_rat_win_rate_bin'] = 0
ratios.loc[ratios['m_rat_win_rate'] < 0.5, 'm_rat_win_rate_bin'] = -1
ratios.loc[ratios['m_rat_win_rate'] >= 0.5, 'm_rat_win_rate_bin'] = 1

# m_rat_ror
ratios['m_rat_ror_bin'] = 0
ratios.loc[ratios['m_rat_ror'] > 0.1, 'm_rat_ror_bin'] = -1
ratios.loc[ratios['m_rat_ror'] <= 0.1, 'm_rat_ror_bin'] = 1

# d_rat_pain
ratios['d_rat_pain_bin'] = 0
ratios.loc[ratios['d_rat_pain'] < 0.5, 'd_rat_pain_bin'] = -1
ratios.loc[ratios['d_rat_pain'] >= 0.5, 'd_rat_pain_bin'] = 1


# Drop columns by their index positions
ratios_bin = ratios.drop(ratios.columns[1:13], axis=1)
ratios_bin.info()

# @title SQLalchemy to push (FE) data to aws db (mysql)


# Write the DataFrame to a new table in the database
ratios_bin.to_sql('FE_RATIOS_SIGNALS', con=gcp_engine, if_exists='replace', index=False)

print("FE_RATIOS_SIGNALS DataFrame uploaded to AWS MySQL database successfully!")

end_time = time.time()
elapsed_time_seconds = end_time - start_time
elapsed_time_minutes = elapsed_time_seconds / 60

print(f"Cell execution time: {elapsed_time_minutes:.2f} minutes")

gcp_engine.dispose()


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
oscillator.to_sql('FE_OSCILLATORS', con=gcp_engine, if_exists='append', index=False)
# Write the DataFrame to a new table in the database
df_oscillator_bin.to_sql('FE_OSCILLATORS_SIGNALS', con=gcp_engine, if_exists='append', index=False)

# Write the DataFrame to a new table in the database
ratios_df.drop(ratios_df.columns[16:27], axis=1, inplace=True)
ratios_df.info()
ratios_df.to_sql('FE_RATIOS', con=gcp_engine, if_exists='append', index=False)
# Write the DataFrame to a new table in the database
ratios_bin.to_sql('FE_RATIOS_SIGNALS', con=gcp_engine, if_exists='append', index=False)


# Write the DataFrame to a new table in the database
momentum_df.to_sql('FE_MOMENTUM', con=gcp_engine, if_exists='append', index=False)
# Write the DataFrame to a new table in the database
df_momentum.to_sql('FE_MOMENTUM_SIGNALS', con=gcp_engine, if_exists='append', index=False)


print("table name to db name append done")

gcp_engine.dispose()
