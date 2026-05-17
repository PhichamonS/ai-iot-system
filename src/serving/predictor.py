import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from datetime import timezone
from dotenv import load_dotenv
import os

from src.preprocessing import run_pipeline
from src.registry.model_registry import ModelRegistry

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

SARIMA_DIR  = os.path.join(os.path.dirname(__file__), "..", "..", "models", "sarima")
XGBOOST_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "models", "xgboost")
METER_IDS   = ['MT_124', 'MT_131', 'MT_132', 'MT_156', 'MT_158']
HORIZON     = 96    # 96 × 15min = next 24 hours


def forecast_sarima(meter_id: str) -> pd.DataFrame:
    registry = ModelRegistry(SARIMA_DIR)
    model, meta = registry.load_latest(meter_id)

    df = run_pipeline(meter_id, days=14)
    model.update(df['consumption_kw'])   # update with latest data

    preds, conf = model.predict(n_periods=HORIZON, return_conf_int=True)

    index = pd.date_range(
        start=df.index[-1] + pd.Timedelta(minutes=15),
        periods=HORIZON, freq='15T', tz='UTC'
    )
    return pd.DataFrame({
        'time':        index,
        'meter_id':    meter_id,
        'forecast_kw': preds,
        'lower_bound': conf[:, 0],
        'upper_bound': conf[:, 1],
        'model':       'sarima'
    })


def forecast_xgboost(meter_id: str, meter_id_enc: int) -> pd.DataFrame:
    registry = ModelRegistry(XGBOOST_DIR)
    model, meta = registry.load_latest('global')

    df = run_pipeline(meter_id, days=14)
    df['meter_id_enc'] = meter_id_enc

    # Use last known row to build forecast window
    last_row = df[meta['feature_names']].iloc[[-1]]
    preds    = []

    for _ in range(HORIZON):
        pred = float(model.predict(last_row)[0])
        preds.append(pred)
        # Shift lag features forward one step
        last_row['lag_15min'] = pred
        last_row['slot']      = (last_row['slot'].values[0] + 1) % 96

    index = pd.date_range(
        start=df.index[-1] + pd.Timedelta(minutes=15),
        periods=HORIZON, freq='15T', tz='UTC'
    )
    return pd.DataFrame({
        'time':        index,
        'meter_id':    meter_id,
        'forecast_kw': preds,
        'lower_bound': np.array(preds) * 0.9,
        'upper_bound': np.array(preds) * 1.1,
        'model':       'xgboost'
    })


def write_forecast_to_db(df: pd.DataFrame):
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"), 
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"), 
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cursor = conn.cursor()
    rows = [
        (row.time.to_pydatetime().replace(tzinfo=timezone.utc),
         row.meter_id, float(row.forecast_kw), row.model)
        for row in df.itertuples()
    ]
    execute_values(cursor, """
        INSERT INTO energy_forecast (time, meter_id, forecast_kw, model)
        VALUES %s
        ON CONFLICT (time, meter_id, model)
        DO UPDATE SET forecast_kw = EXCLUDED.forecast_kw
    """, rows)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"  Wrote {len(rows)} forecast rows → TimescaleDB")


def run_all_forecasts(model_type: str = "sarima"):
    for i, meter_id in enumerate(METER_IDS):
        try:
            if model_type == "sarima":
                df = forecast_sarima(meter_id)
            else:
                df = forecast_xgboost(meter_id, meter_id_enc=i)
            write_forecast_to_db(df)
        except Exception as e:
            print(f"  Forecast failed for {meter_id}: {e}")


if __name__ == "__main__":
    run_all_forecasts(model_type="sarima")