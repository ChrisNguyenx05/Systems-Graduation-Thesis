-- ============================================================================
-- SYS-05: LAKEHOUSE & DATABASE MANAGEMENT PORTAL
-- Complete ClickHouse DDL Script based on Investigation & Google Sheet Specs
-- Covers 4 Categories: Klines, Trades (aggTrade), Order Book Depth, Liquidations
-- Plus Trading Pairs Registry Table from Google Sheet (Popular & New Coins)
-- ============================================================================

-- 1. Klines (Candlesticks) - Real-time & Historical OHLCV Gold Table
CREATE TABLE IF NOT EXISTS default.crypto_klines_gold (
    symbol LowCardinality(String),
    open_time DateTime64(3, 'UTC'),
    close_time DateTime64(3, 'UTC'),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    quote_asset_volume Float64,
    number_of_trades UInt32,
    taker_buy_base_asset_volume Float64 DEFAULT 0.0,
    taker_buy_quote_asset_volume Float64 DEFAULT 0.0,
    sma_20 Float64 DEFAULT 0.0,
    rsi_14 Float64 DEFAULT 0.0,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PRIMARY KEY (symbol, open_time)
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;

-- 2. Materialized View for Hourly Volume Aggregations
CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_hourly_volume
ENGINE = SummingMergeTree()
PRIMARY KEY (symbol, hourly_time)
ORDER BY (symbol, hourly_time)
AS SELECT
    symbol,
    toStartOfHour(open_time) AS hourly_time,
    sum(volume) AS total_volume,
    count() AS total_klines
FROM default.crypto_klines_gold
GROUP BY symbol, hourly_time;

-- 3. Real-time Aggregated Trades (aggTrade) Table
CREATE TABLE IF NOT EXISTS default.crypto_trades_raw (
    symbol LowCardinality(String),
    agg_trade_id UInt64,
    price Float64,
    quantity Float64,
    first_trade_id UInt64,
    last_trade_id UInt64,
    trade_time DateTime64(3, 'UTC'),
    is_buyer_maker UInt8, -- 1 = Taker Seller (SELL), 0 = Taker Buyer (BUY)
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PRIMARY KEY (symbol, trade_time)
ORDER BY (symbol, trade_time, agg_trade_id)
SETTINGS index_granularity = 8192;

-- 4. Real-time Order Book Depth Table
CREATE TABLE IF NOT EXISTS default.crypto_orderbook_depth (
    symbol LowCardinality(String),
    update_id UInt64,
    bids Array(Tuple(Float64, Float64)),
    asks Array(Tuple(Float64, Float64)),
    event_time DateTime64(3, 'UTC'),
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PRIMARY KEY (symbol, event_time)
ORDER BY (symbol, event_time)
SETTINGS index_granularity = 8192;

-- 5. Futures Forced Liquidations Table
CREATE TABLE IF NOT EXISTS default.crypto_liquidations (
    symbol LowCardinality(String),
    side LowCardinality(String), -- BUY (Long Liquidated) or SELL (Short Liquidated)
    price Float64,
    quantity Float64,
    trade_time DateTime64(3, 'UTC'),
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PRIMARY KEY (symbol, trade_time)
ORDER BY (symbol, trade_time)
SETTINGS index_granularity = 8192;

-- 6. Trading Pairs Registry Table (From Google Sheet: Popular & New Coins)
CREATE TABLE IF NOT EXISTS default.crypto_trading_pairs (
    symbol LowCardinality(String),
    base_asset LowCardinality(String),
    quote_asset LowCardinality(String),
    category LowCardinality(String), -- 'popular' or 'new_coin'
    is_active UInt8 DEFAULT 1,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PRIMARY KEY (symbol)
ORDER BY (symbol);

-- 7. Data Quality Results for Pipeline Gates
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

-- 8. Pipeline Batch Status for Cross-System Freshness and Audit
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

-- Seed initial Trading Pairs from Google Sheet (Popular & New Coins)
INSERT INTO default.crypto_trading_pairs (symbol, base_asset, quote_asset, category, is_active) VALUES
    ('BTCUSDT', 'BTC', 'USDT', 'popular', 1),
    ('ETHUSDT', 'ETH', 'USDT', 'popular', 1),
    ('BNBUSDT', 'BNB', 'USDT', 'popular', 1),
    ('AAPLBUSDT', 'AAPLB', 'USDT', 'new_coin', 1),
    ('AMZNBUSDT', 'AMZNB', 'USDT', 'new_coin', 1),
    ('DELLBUSDT', 'DELLB', 'USDT', 'new_coin', 1);

