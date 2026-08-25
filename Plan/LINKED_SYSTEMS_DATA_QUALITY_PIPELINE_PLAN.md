# Linked Systems and Data Quality Pipeline Plan

Project: Financial Analytics and Fraud Detection System  
Date checked: 2026-08-22  
Scope reviewed: implemented SYS-02 and SYS-05, plus the planned SYS-01 to SYS-06 ecosystem

## Goal

Make all systems work as one complete pipeline:

Binance APIs -> scheduled ingestion -> Bronze storage -> Data Quality control -> Silver cleaned data -> Gold analytical tables -> dashboards, ML retraining, monitoring, and alerts.

The central rule should be:

> No downstream dashboard, model, or alert should use data until it passes the Data Quality gate.

## Current State

### SYS-02: Data Pipeline and Orchestration

SYS-02 already has the correct orchestration shape:

- Airflow DAG `01_binance_klines_etl_dag`
- Airflow DAG `02_ml_model_retrain_dag`
- Airflow DAG `03_lakehouse_maintenance_dag`
- Data Quality helper: `validate_klines_data`
- SLA/failure callback hooks
- Operators for lakehouse tasks and ClickHouse tasks

Important limitation:

- The current Data Quality gate validates a mocked in-memory batch.
- The Bronze extraction task is also mocked.
- The ClickHouse load inserts sample rows instead of loading the exact records that passed quality checks.

So SYS-02 is structurally good, but the pipeline contract is not yet strict enough.

### SYS-05: Lakehouse and Database Management

SYS-05 already has stronger real infrastructure helpers:

- MinIO bucket creation and object management
- Bronze, Silver, and Gold bucket helpers
- ClickHouse table initialization
- ClickHouse insert/query helpers for kline data
- ClickHouse schemas for klines, trades, depth, liquidations, and trading pairs
- Integration tests that exercise MinIO and ClickHouse

Important limitation:

- SYS-05 owns the better storage/query clients, but SYS-02 currently duplicates simplified operators.
- There is no shared pipeline status table or quality-result table yet.
- There is no clear quarantine path for failed records.

## Recommended End-to-End Pipeline

### General Binance Pipeline

1. `extract_binance_api_batch`

   Airflow calls Binance REST/WebSocket collectors on schedule.

   Output:

   - Raw JSON/CSV payload
   - Batch ID
   - Symbol
   - Time window
   - Source endpoint

2. `write_bronze_raw`

   Store untouched source payloads in MinIO Bronze.

   Example path:

   `s3://crypto-bronze/binance/klines/symbol=BTCUSDT/date=YYYY-MM-DD/hour=HH/batch_id=<id>.json`

3. `validate_data_quality`

   Run Data Quality before Silver/Gold.

   Checks:

   - Required schema fields exist
   - `open_time` and `close_time` are valid timestamps
   - `open`, `high`, `low`, `close`, `volume` are numeric
   - `close` is not null
   - `volume` is not negative
   - `high >= low`
   - `high >= open`, `high >= close`
   - `low <= open`, `low <= close`
   - duplicate kline keys are detected
   - expected interval continuity is checked
   - API batch is not empty

4. `quarantine_bad_records`

   If quality fails, write failed records to a quarantine bucket/prefix and stop Gold loading.

   Example path:

   `s3://crypto-bronze/quarantine/klines/symbol=BTCUSDT/date=YYYY-MM-DD/batch_id=<id>.json`

5. `write_silver_cleaned`

   Only quality-passed records go to Silver.

   Example path:

   `s3://crypto-silver/binance/klines/symbol=BTCUSDT/date=YYYY-MM-DD/hour=HH/data.parquet`

6. `compute_gold_features`

   Add analytical features:

   - SMA
   - RSI
   - VWAP
   - return percentage
   - volatility window
   - anomaly labels if available

7. `load_gold_clickhouse`

   Insert only quality-approved records into ClickHouse `crypto_klines_gold`.

8. `publish_pipeline_status`

   Store batch-level status:

   - batch ID
   - source
   - symbol
   - record counts
   - quality result
   - Silver path
   - Gold row count
   - Airflow DAG run ID
   - created/updated timestamps

9. `notify_and_monitor`

   Send alerts only when:

   - Binance API is unavailable
   - Data Quality fails
   - no fresh Gold rows are available
   - ClickHouse load fails
   - SLA is missed

## Linked System Responsibilities

### SYS-02: Orchestration Owner

SYS-02 should own:

- schedules
- dependency order
- retries
- SLA rules
- pipeline status
- Data Quality gate execution
- triggering SYS-04 retraining after Gold data is fresh
- triggering SYS-06 alerts when a pipeline fails

SYS-02 should not own duplicate storage/database logic long-term. It should call shared SYS-05 client helpers or a stable internal API.

### SYS-05: Storage and Query Owner

SYS-05 should own:

- MinIO buckets and object paths
- Bronze/Silver/Gold storage conventions
- ClickHouse schemas
- ClickHouse inserts and queries
- retention rules
- quarantine storage
- lakehouse health checks

### SYS-01: Dashboard Consumer

SYS-01 should read only from:

- ClickHouse Gold tables
- quality-approved feature views
- pipeline status tables for freshness badges

SYS-01 should never read raw Bronze data directly.

### SYS-03: Monitoring Consumer

SYS-03 should monitor:

- Airflow DAG status
- batch success/failure counts
- Data Quality pass/fail counts
- MinIO bucket sizes
- ClickHouse freshness
- API collection latency
- disk usage and container health

### SYS-04: ML Consumer

SYS-04 should train only when:

- latest Gold data passes quality
- enough rows are available
- feature windows are complete
- model input schema matches expected version

### SYS-06: Alert Dispatcher

SYS-06 should receive structured alert events from SYS-02/SYS-03:

- `DATA_QUALITY_FAILED`
- `BINANCE_API_FAILED`
- `GOLD_LOAD_FAILED`
- `STALE_GOLD_DATA`
- `SLA_MISSED`
- `MODEL_RETRAIN_FAILED`

## Required Improvements

### P0: Make Data Quality real

Replace the mocked quality batch in `01_binance_klines_etl_dag` with records pulled from the actual Bronze or Silver output of the current DAG run.

Expected result:

- Airflow task validates the same records that will be loaded into ClickHouse.
- Failed data never reaches Gold.
- Quality result is stored as an artifact.

### P0: Define a shared batch ID

Every run should have one batch ID passed through all layers.

Example:

`binance_klines_BTCUSDT_20260822T100000Z`

The batch ID should appear in:

- Bronze object path
- Silver object path
- quality result object
- pipeline status table
- Airflow XCom
- logs and alert messages

### P0: Add quality result storage

Add either a MinIO JSON result or a ClickHouse table.

Recommended ClickHouse table:

```sql
CREATE TABLE IF NOT EXISTS default.data_quality_results (
    batch_id String,
    dataset LowCardinality(String),
    symbol LowCardinality(String),
    check_time DateTime DEFAULT now(),
    passed UInt8,
    total_records UInt64,
    failed_records UInt64,
    null_close_count UInt64,
    negative_volume_count UInt64,
    duplicate_key_count UInt64,
    interval_gap_count UInt64,
    error_summary String
)
ENGINE = MergeTree()
ORDER BY (dataset, symbol, check_time, batch_id);
```

### P1: Use SYS-05 clients from SYS-02

Reduce duplicate logic in SYS-02 operators by reusing SYS-05 helpers:

- `LakehouseStorageManager`
- `ClickHouseManager`

This creates one source of truth for MinIO and ClickHouse behavior.

### P1: Add quarantine

Create a quarantine prefix or bucket:

- `crypto-quarantine`
- or `crypto-bronze/quarantine/...`

Store:

- rejected records
- quality report
- source batch metadata
- failure reason

### P1: Add pipeline status table

Recommended ClickHouse table:

```sql
CREATE TABLE IF NOT EXISTS default.pipeline_batch_status (
    batch_id String,
    pipeline LowCardinality(String),
    symbol LowCardinality(String),
    source_endpoint String,
    bronze_path String,
    silver_path String,
    gold_table String,
    status LowCardinality(String),
    quality_passed UInt8,
    records_bronze UInt64,
    records_silver UInt64,
    records_gold UInt64,
    started_at DateTime,
    finished_at DateTime,
    airflow_dag_run_id String,
    error_message String
)
ENGINE = ReplacingMergeTree(finished_at)
ORDER BY (pipeline, symbol, batch_id);
```

### P2: Make schemas consistent

SYS-02 currently uses a simplified ClickHouse table definition with `timestamp`, while SYS-05 uses the richer `open_time` / `close_time` schema.

Use the SYS-05 schema everywhere:

- `open_time`
- `close_time`
- `quote_asset_volume`
- `number_of_trades`
- `sma_20`
- `rsi_14`

### P2: Add freshness views

Create a view for downstream systems:

```sql
CREATE VIEW IF NOT EXISTS default.v_latest_quality_approved_klines AS
SELECT *
FROM default.crypto_klines_gold
WHERE updated_at >= now() - INTERVAL 24 HOUR;
```

SYS-01 and SYS-04 should consume from stable views rather than raw implementation tables where possible.

## Improved Airflow DAG Shape

Recommended `01_binance_klines_etl_dag`:

```text
start
  -> create_batch_context
  -> extract_binance_klines
  -> write_bronze_raw
  -> validate_bronze_quality
  -> branch_quality_result
      -> quarantine_bad_records -> write_failed_status -> notify_failure -> end_failed
      -> transform_to_silver -> write_silver -> compute_gold_features -> load_gold_clickhouse
  -> verify_gold_freshness
  -> write_success_status
  -> notify_success_if_needed
  -> end_success
```

This makes Data Quality part of the general pipeline, not a separate side check.

## Definition of Complete

The linked ecosystem is complete when:

1. A scheduled Airflow run can collect Binance data.
2. Raw data lands in Bronze with a batch ID.
3. Quality checks run on the collected batch.
4. Bad records are quarantined.
5. Only valid records move to Silver and Gold.
6. ClickHouse Gold contains fresh quality-approved rows.
7. SYS-01 can show the latest Gold data and freshness status.
8. SYS-04 retraining is blocked if Gold data is stale or quality failed.
9. SYS-03 can monitor pipeline and quality metrics.
10. SYS-06 can alert on API failure, quality failure, and stale Gold data.

## Improvements Implemented on 2026-08-22

This pass completed the first foundation improvements:

- Upgraded `validate_klines_data` from a simple null/negative-volume check into a Binance OHLCV quality gate.
- Added validation for required fields, numeric fields, timestamp presence, impossible OHLC candles, duplicate symbol/timestamp keys, and missing interval gaps.
- Added SYS-02 unit tests for invalid OHLC shape, duplicate kline keys, and interval gaps.
- Added ClickHouse DDL for `data_quality_results` and `pipeline_batch_status` in SYS-05 SQL bootstrap.
- Mirrored those DDL tables inside the SYS-05 Python ClickHouse initializer.

Remaining next step: wire the Airflow DAG to validate the real collected Binance batch instead of a mocked in-memory batch.
## Company Pipeline Control Portal

The portal should be treated as the company's operations portal, not only a navigation page.

Portal responsibilities:

- Show one row per pipeline batch/run.
- Filter by date, product, seller/source, and pipeline status.
- Show Collect, Data Quality, Transform, and Pipeline status separately.
- Expose the batch ID for traceability across Airflow, MinIO, ClickHouse, and alerts.
- Link operators directly to Airflow for rerun/debug actions.
- Export filtered run history for thesis reports or operating evidence.
- Later read from `pipeline_batch_status` and `data_quality_results` through a small API instead of static sample rows.

Implemented portal foundation:

- Added a `Company Pipeline Control Portal` board to `portal.html`.
- Added representative rows for Klines, aggTrade, depth snapshots, and liquidations.
- Added filters, status metrics, export, compact columns, and Airflow action links.
## Recommended Next Build Order

1. Upgrade `validate_klines_data` to full OHLCV and interval checks.
2. Add `data_quality_results` and `pipeline_batch_status` tables to SYS-05 DDL.
3. Refactor SYS-02 DAG 01 so it passes real batch records through XCom or object paths.
4. Add quarantine write support.
5. Align SYS-02 ClickHouse operator with SYS-05 ClickHouse schema.
6. Add integration test: Bronze sample -> quality pass -> Silver write -> Gold insert.
7. Add integration test: invalid Binance sample -> quality fail -> quarantine -> no Gold insert.
8. Add dashboard freshness/status endpoint or query for SYS-01.
9. Add alert event contract for SYS-06.
10. Add cloud compose networking so all services use container DNS names in deployment.



