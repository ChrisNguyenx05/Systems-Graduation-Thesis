# LightGBM Fraud Detector

Train from the labelled historical IEEE-CIS dataset:

```powershell
python train.py
```

The pipeline uses a chronological `TransactionDT` 80/20 holdout, not a random split. It saves the model, exact feature transformer, feature list, and metric report under `AI Models/models/LightGBM Fraud Detector/`.

This is a candidate model. It must be revalidated on the future `gold_transaction_risk_features + fraud_labels` production contract before promotion.
