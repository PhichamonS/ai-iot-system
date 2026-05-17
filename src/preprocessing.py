import pandas as pd
import numpy as np
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

FREQ     = '15min'   # ← 15-minute intervals
LAG_1    = 1       # 15 min ago
LAG_DAY  = 96      # 24 hours ago  (96 × 15min)
LAG_WEEK = 672     # 7 days ago    (672 × 15min)
ROLL_6H  = 24      # 6-hour window (24 × 15min)
ROLL_24H = 96      # 24-hour window


def get_raw_data(meter_id: str, days: int = 30) -> pd.DataFrame:
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"), 
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"), 
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    df = pd.read_sql(f"""  
        SELECT time, meter_id, consumption_kw
        FROM energy_data
        WHERE meter_id = %s
          AND time >= NOW() - INTERVAL '{days} days'
        ORDER BY time
    """, conn, params=(meter_id,))
    conn.close()
    df.set_index('time', inplace=True)
    df.index = pd.to_datetime(df.index, utc=True)
    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df[~df.index.duplicated(keep='first')]
    print(f"Duplicates removed: {before - len(df)}")
    return df


def fix_missing_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    full_index = pd.date_range(
        start=df.index.min(),
        end=df.index.max(),
        freq=FREQ, tz='UTC'       # ← 15T
    )
    df = df.reindex(full_index)
    missing = df['consumption_kw'].isna().sum()
    print(f"Missing timestamps filled: {missing}")

    # Interpolate short gaps up to 4 steps (1 hour)
    df['consumption_kw'] = df['consumption_kw'].interpolate(
        method='time', limit=4    # ← 4 × 15min = 1 hour max
    )
    # Forward fill remaining gaps up to 8 steps (2 hours)
    df['consumption_kw'] = df['consumption_kw'].fillna(
        method='ffill', limit=8   # ← 8 × 15min = 2 hours max
    )
    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    window    = ROLL_24H          # ← 96-step rolling window (24 hrs)
    threshold = 3.0

    rolling_mean = df['consumption_kw'].rolling(window=window, center=True).mean()
    rolling_std  = df['consumption_kw'].rolling(window=window, center=True).std()

    z_scores     = (df['consumption_kw'] - rolling_mean) / rolling_std
    outlier_mask = z_scores.abs() > threshold

    print(f"Outliers replaced: {outlier_mask.sum()}")
    df.loc[outlier_mask, 'consumption_kw'] = rolling_mean[outlier_mask]
    return df


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    df['hour']       = df.index.hour
    df['minute']     = df.index.minute          # ← new: 0,15,30,45
    df['dayofweek']  = df.index.dayofweek
    df['is_weekend'] = df['dayofweek'].isin([5, 6]).astype(int)
    df['month']      = df.index.month

    # Time-of-day slot: 0-95 (each 15min block in a day)
    df['slot'] = df['hour'] * 4 + df['minute'] // 15   # ← new

    # Lag features (now in 15-min steps)
    df['lag_15min']  = df['consumption_kw'].shift(LAG_1)
    df['lag_24h']    = df['consumption_kw'].shift(LAG_DAY)   # 96 steps
    df['lag_1week']  = df['consumption_kw'].shift(LAG_WEEK)  # 672 steps

    # Rolling statistics
    df['rolling_mean_6h']  = df['consumption_kw'].rolling(ROLL_6H).mean()
    df['rolling_mean_24h'] = df['consumption_kw'].rolling(ROLL_24H).mean()
    df['rolling_std_24h']  = df['consumption_kw'].rolling(ROLL_24H).std()

    return df.dropna()


def run_pipeline(meter_id: str, days: int = 30) -> pd.DataFrame:
    print(f"\n--- Pipeline for {meter_id} ---")

    df = get_raw_data(meter_id, days)
    print(f"Raw rows:   {len(df)}")

    df = remove_duplicates(df)
    df = fix_missing_timestamps(df)
    df = remove_outliers(df)
    df = add_features(df)

    print(f"Clean rows: {len(df)}")
    print(f"Expected:   {days * LAG_DAY} rows ({days} days × {LAG_DAY} slots/day)")
    return df