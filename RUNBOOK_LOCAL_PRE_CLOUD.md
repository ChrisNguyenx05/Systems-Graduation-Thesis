# Local Operations Runbook: SYS-02, SYS-05, and SYS-06

Use this guide to start the three data-engineering systems, prove that they work together locally, and decide whether they are ready for cloud deployment.

## 1. Before starting

1. Open Docker Desktop and wait for **Engine running**.
2. Open PowerShell in `E:\Graduation_Thesis`.
3. Do not run `docker compose down -v`. The `-v` option deletes persistent database and lakehouse data.
4. Keep secrets only in each system's `.env` file. Do not paste them into source code or commit them.

## 2. Start the systems

Start in this order: storage first, orchestration second, alerts third.

```powershell
Set-Location E:\Graduation_Thesis\SYS-05
docker compose up -d

Set-Location E:\Graduation_Thesis\SYS-02
docker compose up -d --build

Set-Location E:\Graduation_Thesis\SYS-06
docker compose -f docker-compose.v2.yml up -d --build
```

Wait about one minute, then check all containers:

```powershell
docker ps
```

Expected running containers:

| System | Containers | Healthy result |
|---|---|---|
| SYS-02 | `sys02_airflow_webserver`, `sys02_airflow_scheduler`, `sys02_airflow_postgres` | Webserver and Postgres report `healthy`; scheduler reports `Up`. |
| SYS-05 | `minio_s3`, `clickhouse_server`, `sys05_postgres` | All report `healthy`. |
| SYS-06 | `sys-06-sys06-alert-dispatcher-v2-1` (name may differ slightly) | Reports `healthy`. |

## 3. Localhost pages

| Purpose | URL | What to check |
|---|---|---|
| SYS-02 Airflow | http://localhost:8085 | Log in with the account in `SYS-02/.env`; the DAG `01_binance_klines_etl_dag` is visible. |
| SYS-05 MinIO console | http://localhost:9001 | Buckets `crypto-bronze`, `crypto-silver`, and `crypto-gold` exist. |
| SYS-05 ClickHouse SQL UI | http://localhost:8123/play | SQL editor opens. |
| SYS-06 health API | http://localhost:8080/health | JSON response has a healthy service state and `dry_run` value. |

`http://localhost:8080/` returning `{"detail":"Not found"}` is normal. SYS-06 exposes `/health` and `/alerts`, not a homepage.

## 4. Run and verify SYS-02 ETL

1. Open Airflow at http://localhost:8085.
2. Open DAG **`01_binance_klines_etl_dag`**.
3. Click the play button, then choose **Trigger DAG**.
4. Open the newest DAG run in **Grid** view.
5. Wait until every task is green:

   - `extract_bronze_data`
   - `transform_silver_parquet`
   - `validate_data_quality_gate`
   - `load_gold_clickhouse`

If a task is red, click it and choose **Log**. Do not mark a failed task successful manually; read and fix its error first.

Command-line check for the latest run:

```powershell
docker exec sys02_airflow_scheduler airflow dags list-runs --dag-id 01_binance_klines_etl_dag --output json
```

The newest run must have `"state": "success"`.

## 5. Verify data in SYS-05

### MinIO: Bronze and Silver files

Open http://localhost:9001, sign in using values from `SYS-05/.env`, then check:

1. `crypto-bronze` contains a new object under `klines/symbol=BTCUSDT/...`.
2. `crypto-silver` contains a processed object for the same batch.

The object time should match the Airflow run time. If it does not, reopen the Airflow task log before rerunning.

### ClickHouse: Gold rows

Open http://localhost:8123/play and run:

```sql
SELECT
  count() AS total_rows,
  max(updated_at) AS newest_write
FROM crypto_klines_gold;

SELECT symbol, open_time, close, volume, updated_at
FROM crypto_klines_gold
ORDER BY updated_at DESC
LIMIT 20;
```

Success means the row count is above zero and `newest_write` is close to the latest successful Airflow run. This proves the path **SYS-02 -> MinIO -> ClickHouse (SYS-05)** is real, not a mock success.

## 6. Verify SYS-06 alerts and Telegram

### Health and logs

```powershell
Invoke-RestMethod http://localhost:8080/health

Set-Location E:\Graduation_Thesis\SYS-06
docker compose -f docker-compose.v2.yml logs --tail 100
```

For live log streaming, use:

```powershell
docker compose -f docker-compose.v2.yml logs -f
```

Press `Ctrl+C` to stop viewing logs; this does not stop the container.

### Safe dry-run test

When `ALERT_DRY_RUN=true`, SYS-06 accepts and records the alert but intentionally does **not** send a Telegram message. This is the correct first test before using real credentials.

```powershell
$body = @{
  alert_type = 'PIPELINE_FAILURE'
  level = 'WARNING'
  source_system = 'SYS-02'
  event = 'LOCAL_SMOKE_TEST'
  deduplicate_key = 'sys02-local-smoke-test-001'
  message = 'Local SYS-02 to SYS-06 smoke test'
  channels = @('telegram')
} | ConvertTo-Json

Invoke-RestMethod -Method Post -Uri http://localhost:8080/alerts -ContentType 'application/json' -Body $body
```

Expected result: HTTP `202` and a JSON response describing the accepted dispatch. Check the SYS-06 logs for the same alert.

### Live Telegram test

Only after the dry run succeeds:

1. In `SYS-06/.env`, set `ALERT_DRY_RUN=false` and provide valid `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, and a long unique `ALERT_API_TOKEN`.
2. Restart SYS-06:

   ```powershell
   Set-Location E:\Graduation_Thesis\SYS-06
   docker compose -f docker-compose.v2.yml up -d --force-recreate
   ```

3. Send the same test, adding the API token header without exposing the token in logs:

   ```powershell
   $headers = @{ 'X-Alert-Token' = $env:ALERT_API_TOKEN }
   Invoke-RestMethod -Method Post -Uri http://localhost:8080/alerts -Headers $headers -ContentType 'application/json' -Body $body
   ```

4. Confirm one Telegram message arrives and the API response reports successful Telegram delivery.

If no message arrives, check these in order: `ALERT_DRY_RUN` is `false`, the container was recreated, the chat ID is correct, then inspect `docker compose ... logs --tail 100`.

## 7. Quick failure checks

| Symptom | First check |
|---|---|
| `localhost:8085` does not open | `docker logs sys02_airflow_webserver --tail 100` |
| Airflow Gold task retries | Task log in Airflow, then `docker logs clickhouse_server --tail 100` |
| Files do not appear in MinIO | Airflow `extract_bronze_data` and `transform_silver_parquet` logs |
| ClickHouse data is not new | Run the SQL in section 5 and inspect `load_gold_clickhouse` log |
| SYS-06 says `Not found` on port 8080 | Use `/health`, not `/` |
| Telegram notification does not arrive | Check `dry_run`, then SYS-06 logs and Telegram credentials |

## 8. Pre-cloud deployment gate

Do not deploy until every item is true:

- [ ] `docker ps` shows all required containers running and healthy.
- [ ] A new Airflow run is fully green.
- [ ] Its Bronze and Silver objects exist in MinIO.
- [ ] Its Gold data appears in ClickHouse with a recent `updated_at` value.
- [ ] `GET /health` for SYS-06 returns HTTP 200.
- [ ] SYS-06 dry-run alert succeeds; live Telegram alert also succeeds when enabled.
- [ ] `E:\Graduation_Thesis\Test\venv\Scripts\python.exe -m pytest E:\Graduation_Thesis\SYS-06\tests -q` passes.
- [ ] `.env` files are excluded from Git; cloud secrets are stored in the cloud provider's secret manager.
- [ ] Persistent volumes/databases have a tested backup and restore procedure.
- [ ] Public cloud ports are restricted: do not expose MinIO, ClickHouse, Postgres, or Airflow directly to the internet.

## 9. Stop safely

Stopping containers preserves local data:

```powershell
Set-Location E:\Graduation_Thesis\SYS-06
docker compose -f docker-compose.v2.yml down

Set-Location E:\Graduation_Thesis\SYS-02
docker compose down

Set-Location E:\Graduation_Thesis\SYS-05
docker compose down
```

Do not add `-v` unless you intentionally want to erase local persistent data.
