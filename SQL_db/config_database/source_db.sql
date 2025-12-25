
CREATE TABLE IF NOT EXISTS trades (
    symbol VARCHAR,
    tradeId VARCHAR,
    side VARCHAR,
    price DOUBLE,
    quantity DOUBLE,
    trade_time TIMESTAMP,
    ingestion_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_books (
    symbol VARCHAR,
    side VARCHAR,
    price DOUBLE,
    quantity DOUBLE,
    snapshot_time TIMESTAMP,
    ingestion_time TIMESTAMP -- Nếu code python chưa có thì comment lại
);

CREATE TABLE IF NOT EXISTS funding_rate (
    symbol VARCHAR,
    instrument_type VARCHAR,
    funding_rate DOUBLE,
    next_funding_rate DOUBLE,
    funding_time TIMESTAMP,
    next_funding_time TIMESTAMP,
    ingestion_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ohlc_mark (
    symbol VARCHAR,
    channel VARCHAR,
    candle_time TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    ingestion_time TIMESTAMP,
    type VARCHAR -- Đổi thành VARCHAR cho dễ ("mark" / "index")
);

CREATE TABLE IF NOT EXISTS ohlc_index (
    symbol VARCHAR,
    channel VARCHAR,
    candle_time TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    ingestion_time TIMESTAMP,
    type VARCHAR
);

-- CREATE TABLE IF NOT EXISTS ohlc (
--     symbol VARCHAR,
--     candle_time TIMESTAMP,
--     open DOUBLE,
--     high DOUBLE,
--     low DOUBLE,
--     close DOUBLE,
--     volume DOUBLE,
--     interval VARCHAR
-- );
