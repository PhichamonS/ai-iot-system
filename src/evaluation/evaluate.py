import json, os
import numpy as np
from datetime import datetime

REPORTS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)


def compute_metrics(y_true, y_pred, model_name: str, meter_id: str) -> dict:
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)

    mae  = float(np.mean(np.abs(y_true - y_pred)))
    rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))

    # Avoid division by zero for MAPE
    mask = y_true != 0
    mape = float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100)

    metrics = {
        "model":      model_name,
        "meter_id":   meter_id,
        "mae":        round(mae,  4),
        "rmse":       round(rmse, 4),
        "mape":       round(mape, 2),
        "trained_at": datetime.utcnow().isoformat(),
        "grade":      grade(mape)
    }

    _append_report(metrics)
    return metrics


def grade(mape: float) -> str:
    if mape < 5:   return "excellent"
    if mape < 10:  return "good"
    if mape < 20:  return "acceptable"
    return "poor"


def compare_models(report_path: str = None) -> dict:
    path = report_path or os.path.join(REPORTS_DIR, "evaluation.jsonl")
    results = []
    with open(path) as f:
        for line in f:
            results.append(json.loads(line))

    # Group by meter_id, pick best model per meter by lowest MAPE
    best = {}
    for r in results:
        mid = r['meter_id']
        if mid not in best or r['mape'] < best[mid]['mape']:
            best[mid] = r

    print("\n=== Best model per meter ===")
    for mid, r in best.items():
        print(f"  {mid}: {r['model']} | MAPE={r['mape']}% | Grade={r['grade']}")

    return best


def _append_report(metrics: dict):
    path = os.path.join(REPORTS_DIR, "evaluation.jsonl")
    with open(path, 'a') as f:
        f.write(json.dumps(metrics) + "\n")