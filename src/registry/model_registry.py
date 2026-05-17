import os, json, joblib
from datetime import datetime


class ModelRegistry:
    """Save, version, and load trained models with metadata."""

    def __init__(self, model_dir: str):
        self.model_dir = model_dir
        os.makedirs(model_dir, exist_ok=True)

    def save(self, model, model_type: str, meter_id: str,
             metrics: dict, **extra_meta):
        version   = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename  = f"{meter_id}_{version}.pkl"
        filepath  = os.path.join(self.model_dir, filename)

        # Save model artifact
        joblib.dump(model, filepath)

        # Save metadata alongside it
        meta = {
            "model_type":  model_type,
            "meter_id":    meter_id,
            "version":     version,
            "filename":    filename,
            "metrics":     metrics,
            **extra_meta
        }
        meta_path = filepath.replace(".pkl", "_meta.json")
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)

        # Update pointer to latest model for this meter
        self._write_latest_pointer(meter_id, filename)

        print(f"  Saved: {filepath}")
        return filepath

    def load_latest(self, meter_id: str):
        pointer_path = os.path.join(self.model_dir, f"{meter_id}_latest.txt")
        if not os.path.exists(pointer_path):
            raise FileNotFoundError(f"No model found for {meter_id}")

        with open(pointer_path) as f:
            filename = f.read().strip()

        filepath  = os.path.join(self.model_dir, filename)
        meta_path = filepath.replace(".pkl", "_meta.json")

        model = joblib.load(filepath)
        with open(meta_path) as f:
            meta = json.load(f)

        print(f"  Loaded: {filename} | MAPE={meta['metrics']['mape']}%")
        return model, meta

    def load_version(self, meter_id: str, version: str):
        filename = f"{meter_id}_{version}.pkl"
        filepath = os.path.join(self.model_dir, filename)
        return joblib.load(filepath)

    def list_versions(self, meter_id: str) -> list:
        files = [f for f in os.listdir(self.model_dir)
                 if f.startswith(meter_id) and f.endswith("_meta.json")]
        versions = []
        for f in sorted(files):
            with open(os.path.join(self.model_dir, f)) as fp:
                versions.append(json.load(fp))
        return versions

    def _write_latest_pointer(self, meter_id: str, filename: str):
        pointer_path = os.path.join(self.model_dir, f"{meter_id}_latest.txt")
        with open(pointer_path, "w") as f:
            f.write(filename)