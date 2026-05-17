import os, joblib
from datetime import datetime
from pmdarima import auto_arima
from src.preprocessing import run_pipeline
from src.evaluation.evaluate import compute_metrics
from src.registry.model_registry import ModelRegistry

MODELS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "models", "sarima")
METER_IDS  = METER_IDS  = ['MT_124', 'MT_131', 'MT_132', 'MT_156', 'MT_158']
N_WINS = 96  # = 24 hours (15min * 96)

def train_sarima(meter_id: str, days: int = 30):
    print(f"\n=== Training SARIMA for {meter_id} ===")

    df = run_pipeline(meter_id, days=days)

    # Train / test split 
    train = df['consumption_kw'].iloc[:-N_WINS]
    test  = df['consumption_kw'].iloc[-N_WINS:]

    model = auto_arima(
        train,
        seasonal=True,
        m=N_WINS,                    
        stepwise=True,
        suppress_warnings=True,
        error_action='ignore'
    )

    preds = model.predict(n_periods=96)
    metrics = compute_metrics(test.values, preds, model_name="sarima", meter_id=meter_id)

    registry = ModelRegistry(model_dir=MODELS_DIR)
    registry.save(
        model=model,
        model_type="sarima",
        meter_id=meter_id,
        metrics=metrics
    )

    print(f"  MAE:  {metrics['mae']:.3f} kW")
    print(f"  MAPE: {metrics['mape']:.2f}%")
    return model, metrics


def train_all_meters():
    results = {}
    for meter_id in METER_IDS:
        try:
            _, metrics = train_sarima(meter_id)
            results[meter_id] = metrics
        except Exception as e:
            print(f"  Failed for {meter_id}: {e}")
    return results


if __name__ == "__main__":
    train_all_meters()