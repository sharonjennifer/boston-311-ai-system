import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

APP_DIR = Path(__file__).resolve().parent
MODEL_DIR = APP_DIR / "models"

exp_path = MODEL_DIR / "priority_xgb_experiments.json"
if not exp_path.exists():
    raise FileNotFoundError(
        f"Experiment file not found at {exp_path}. "
        "Run train_priority_xgb.py first."
    )

with open(exp_path) as f:
    runs = json.load(f)

if not runs:
    raise RuntimeError("No experiment runs found in priority_xgb_experiments.json")

names = [r["name"] for r in runs]
val_pr = [r["val_pr_auc"] for r in runs]
val_roc = [r["val_roc_auc"] for r in runs]

# Plot PR-AUC per model
x = np.arange(len(names))

plt.figure(figsize=(8, 5))
plt.bar(x, val_pr)
plt.xticks(x, names, rotation=20)
plt.ylabel("Validation PR-AUC")
plt.title("Model Comparison: Validation PR-AUC by Hyperparameter Config")
plt.tight_layout()

out_pr_path = MODEL_DIR / "model_comparison_val_pr_auc.png"
plt.savefig(out_pr_path)
plt.close()

plt.figure(figsize=(8, 5))
plt.bar(x, val_roc)
plt.xticks(x, names, rotation=20)
plt.ylabel("Validation ROC-AUC")
plt.title("Model Comparison: Validation ROC-AUC by Hyperparameter Config")
plt.tight_layout()

out_roc_path = MODEL_DIR / "model_comparison_val_roc_auc.png"
plt.savefig(out_roc_path)
plt.close()

print(f"[OK] Saved hyperparameter sensitivity plots to:")
print(f"  - {out_pr_path}")
print(f"  - {out_roc_path}")
