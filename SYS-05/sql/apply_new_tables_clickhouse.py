import clickhouse_connect

c = clickhouse_connect.get_client(host='127.0.0.1', port=8123, username='nhfinanceadmin', password='nhathuy123')

ddls = [
    # 1. db_bronze new tables
    """
    CREATE TABLE IF NOT EXISTS db_bronze.raw_binance_klines_api (
        pair_id UInt8,
        symbol LowCardinality(String),
        market_type LowCardinality(String),
        interval LowCardinality(String),
        source_type LowCardinality(String) DEFAULT 'API',
        ingestion_time DateTime64(3, 'UTC') DEFAULT now64(3),
        raw_payload String
    ) ENGINE = MergeTree()
    PARTITION BY (toYYYYMM(ingestion_time), market_type)
    ORDER BY (symbol, interval, ingestion_time);
    """,
    """
    CREATE TABLE IF NOT EXISTS db_bronze.raw_binance_klines_wsis (
        pair_id UInt8,
        symbol LowCardinality(String),
        market_type LowCardinality(String),
        interval LowCardinality(String),
        source_type LowCardinality(String) DEFAULT 'WEBSOCKET',
        ingestion_time DateTime64(3, 'UTC') DEFAULT now64(3),
        raw_payload String
    ) ENGINE = MergeTree()
    PARTITION BY (toYYYYMM(ingestion_time), market_type)
    ORDER BY (symbol, interval, ingestion_time);
    """,

    # 2. db_silver_market new tables
    """
    CREATE TABLE IF NOT EXISTS db_silver_market.wsis_silver (
        pair_id UInt8,
        market_type LowCardinality(String),
        symbol LowCardinality(String),
        interval LowCardinality(String),
        open_time DateTime64(3, 'UTC'),
        close_time DateTime64(3, 'UTC'),
        open Float64,
        high Float64,
        low Float64,
        close Float64,
        volume Float64,
        quote_asset_volume Float64,
        number_of_trades UInt32,
        taker_buy_base_volume Float64,
        taker_buy_quote_volume Float64,
        first_trade_id Int64 DEFAULT -1,
        last_trade_id Int64 DEFAULT -1,
        is_kline_closed UInt8 DEFAULT 1,
        source LowCardinality(String) DEFAULT 'wsis_stream',
        type_method LowCardinality(String) DEFAULT 'WEBSOCKET',
        ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
    ) ENGINE = ReplacingMergeTree(ingested_at)
    PARTITION BY (market_type, toYYYYMM(open_time))
    PRIMARY KEY (symbol, interval, open_time)
    ORDER BY (symbol, interval, open_time);
    """,

    # 3. Alter silver_crypto_klines in db_silver_market to have source and type_method
    "ALTER TABLE db_silver_market.silver_crypto_klines ADD COLUMN IF NOT EXISTS source LowCardinality(String) DEFAULT 'v1.0.0_api_klines'",
    "ALTER TABLE db_silver_market.silver_crypto_klines ADD COLUMN IF NOT EXISTS type_method LowCardinality(String) DEFAULT 'API'",

    # 4. Tables in default database for backward compatibility and direct queries
    """
    CREATE TABLE IF NOT EXISTS default.silver_crypto_klines (
        pair_id UInt8 DEFAULT 0,
        symbol LowCardinality(String),
        open_time DateTime64(3, 'UTC'),
        close_time DateTime64(3, 'UTC'),
        open Float64,
        high Float64,
        low Float64,
        close Float64,
        volume Float64,
        quote_asset_volume Float64 DEFAULT 0,
        number_of_trades UInt32 DEFAULT 0,
        taker_buy_base_volume Float64 DEFAULT 0,
        taker_buy_quote_volume Float64 DEFAULT 0,
        first_trade_id Int64 DEFAULT -1,
        last_trade_id Int64 DEFAULT -1,
        is_kline_closed UInt8 DEFAULT 1,
        source LowCardinality(String) DEFAULT 'v1.0.0_api_klines',
        type_method LowCardinality(String) DEFAULT 'API',
        ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
    )
    ENGINE = ReplacingMergeTree(ingested_at)
    PRIMARY KEY (symbol, open_time)
    ORDER BY (symbol, open_time);
    """,
    """
    CREATE TABLE IF NOT EXISTS default.wsis_silver (
        pair_id UInt8 DEFAULT 0,
        symbol LowCardinality(String),
        open_time DateTime64(3, 'UTC'),
        close_time DateTime64(3, 'UTC'),
        open Float64,
        high Float64,
        low Float64,
        close Float64,
        volume Float64,
        quote_asset_volume Float64 DEFAULT 0,
        number_of_trades UInt32 DEFAULT 0,
        taker_buy_base_volume Float64 DEFAULT 0,
        taker_buy_quote_volume Float64 DEFAULT 0,
        first_trade_id Int64 DEFAULT -1,
        last_trade_id Int64 DEFAULT -1,
        is_kline_closed UInt8 DEFAULT 1,
        source LowCardinality(String) DEFAULT 'wsis_stream',
        type_method LowCardinality(String) DEFAULT 'WEBSOCKET',
        ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
    )
    ENGINE = ReplacingMergeTree(ingested_at)
    PRIMARY KEY (symbol, open_time)
    ORDER BY (symbol, open_time);
    """,
    """
    CREATE TABLE IF NOT EXISTS default.raw_binance_klines_api (
        pair_id UInt8 DEFAULT 0,
        symbol LowCardinality(String),
        market_type LowCardinality(String) DEFAULT 'SPOT',
        interval LowCardinality(String) DEFAULT '1m',
        source_type LowCardinality(String) DEFAULT 'API',
        ingestion_time DateTime64(3, 'UTC') DEFAULT now64(3),
        raw_payload String
    )
    ENGINE = MergeTree()
    ORDER BY (symbol, interval, ingestion_time);
    """,
    """
    CREATE TABLE IF NOT EXISTS default.raw_binance_klines_wsis (
        pair_id UInt8 DEFAULT 0,
        symbol LowCardinality(String),
        market_type LowCardinality(String) DEFAULT 'SPOT',
        interval LowCardinality(String) DEFAULT '15m',
        source_type LowCardinality(String) DEFAULT 'WEBSOCKET',
        ingestion_time DateTime64(3, 'UTC') DEFAULT now64(3),
        raw_payload String
    )
    ENGINE = MergeTree()
    ORDER BY (symbol, interval, ingestion_time);
    """
]

for stmt in ddls:
    stmt = stmt.strip()
    if stmt:
        c.command(stmt)
        print("Executed:", stmt.splitlines()[0][:60])

print("\n--- Current Tables in db_bronze ---")
print(c.command("SHOW TABLES FROM db_bronze"))

print("\n--- Current Tables in db_silver_market ---")
print(c.command("SHOW TABLES FROM db_silver_market"))

print("\n--- Current Tables in default ---")
print(c.command("SHOW TABLES FROM default"))
