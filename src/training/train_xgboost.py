import os, joblib
import pandas as pd
from xgboost import XGBRegressor
from sklearn.model_selection import TimeSeriesSplit
from src.preprocessing import run_pipeline
from src.evaluation.evaluate import compute_metrics
from src.registry.model_registry import ModelRegistry

MODELS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "models", "xgboost")
METER_IDS  = ['MT_124', 'MT_131', 'MT_132', 'MT_156', 'MT_158']

FEATURES = [
    'hour', 'minute', 'slot', 'dayofweek', 'is_weekend', 'month',
    'lag_15min', 'lag_24h', 'lag_1week',
    'rolling_mean_6h', 'rolling_mean_24h', 'rolling_std_24h',
    'meter_id_enc'
]

def build_global_dataset(days: int = 60) -> pd.DataFrame:
    frames = []
    for i, mid in enumerate(METER_IDS):
        df = run_pipeline(mid, days=days)
        df['meter_id']     = mid
        df['meter_id_enc'] = i       # numeric encoding
        frames.append(df)
    return pd.concat(frames).sort_index()


def train_xgboost(days: int = 60):
    print("\n=== Training global XGBoost ===")

    df = build_global_dataset(days=days)

    # Chronological split — last 96 steps per meter as test
    split_time = df.index.unique().sort_values()[-96]
    train_df   = df[df.index < split_time]
    test_df    = df[df.index >= split_time]

    X_train, y_train = train_df[FEATURES], train_df['consumption_kw']
    X_test,  y_test  = test_df[FEATURES],  test_df['consumption_kw']

    model = XGBRegressor(
        n_estimators=500,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        early_stopping_rounds=20,
        eval_metric='rmse',
        random_state=42
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=50
    )

    preds   = model.predict(X_test)
    metrics = compute_metrics(y_test.values, preds, model_name="xgboost", meter_id="global")

    registry = ModelRegistry(model_dir=MODELS_DIR)
    registry.save(
        model=model,
        model_type="xgboost",
        meter_id="global",
        metrics=metrics,
        feature_names=FEATURES
    )

    print(f"  MAE:  {metrics['mae']:.3f} kW")
    print(f"  MAPE: {metrics['mape']:.2f}%")
    return model, metrics


if __name__ == "__main__":
    train_xgboost()