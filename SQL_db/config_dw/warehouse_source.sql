INSTALL httpfs;
LOAD httpfs;
SET s3_endpoint='minio:9000';
SET s3_access_key_id='minio';
SET s3_secret_access_key='minio123';
SET s3_use_ssl=false;
SET s3_url_style='path';
-- Dimension: Cặp giao dịch (Symbol)
CREATE SEQUENCE IF NOT EXISTS seq_symbol_id START 1;
CREATE TABLE IF NOT EXISTS dim_symbol (
    symbol_key INTEGER DEFAULT nextval('seq_symbol_id') PRIMARY KEY,
    symbol_code VARCHAR UNIQUE,
    base_currency VARCHAR,
    quote_currency VARCHAR
);

-- Fact: Trades
CREATE TABLE IF NOT EXISTS fact_trades (
    symbol_key INTEGER,
    --date_key INTEGER,
    trade_id VARCHAR,
    price DOUBLE,
    quantity DOUBLE,
    trade_time TIMESTAMP,
    PRIMARY KEY (symbol_key, trade_time, trade_id)
);

-- Fact: OHLC (Mark/Index/Calculated đều gom về đây hoặc tách riêng tùy nhu cầu)
-- Ở đây ví dụ tách riêng cho Calculated OHLC
-- CREATE TABLE IF NOT EXISTS fact_ohlc_calculated (
--     symbol_key INTEGER,
--     date_key INTEGER,
--     interval VARCHAR,
--     candle_time TIMESTAMP,
--     open DOUBLE,
--     high DOUBLE,
--     low DOUBLE,
--     close DOUBLE,
--     volume DOUBLE
-- );

-- Fact: Funding Rate
CREATE TABLE IF NOT EXISTS fact_funding_rate (
    symbol_key INTEGER,
    --date_key INTEGER,
    funding_rate DOUBLE,
    next_funding_rate DOUBLE,
    funding_time TIMESTAMP,
    PRIMARY KEY (symbol_key, funding_time)
);