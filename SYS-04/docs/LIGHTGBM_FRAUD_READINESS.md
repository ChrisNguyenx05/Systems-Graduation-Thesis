# LightGBM Fraud Readiness

## Current offline result

`LGBM-FRAUD-02` is trained from the historical IEEE-CIS dataset using 35 features and a chronological `TransactionDT` 80/20 holdout. Its saved JSON report is the authoritative metric record.

This is a **benchmark candidate**, not a production fraud model. IEEE-CIS fields such as `card1`, `C1`, and `V307` are not emitted by the proposed transaction API; the model must not score production payloads from a different schema.

## Production completion gate

1. Apply `SYS-05/sql/02_fraud_transaction_feature_store.sql`.
2. Ingest real or explicitly simulated transactions into `raw_transactions`.
3. Create immutable `gold_transaction_risk_features` snapshots.
4. Store only confirmed outcomes in `fraud_labels`.
5. Train a new `fraud-transaction-v1` model from `v_lightgbm_fraud_training` with a time-based holdout.
6. Promote only if the measured metrics and false-positive cost meet the agreed operating gate.

The WebSocket/API market reconciliation can provide `market_anomaly_score` and `price_volatility_1h`; it never supplies a fraud label.
