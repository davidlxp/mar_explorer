-- Create logs table
CREATE TABLE IF NOT EXISTS logs (
    ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    question VARCHAR,
    response VARCHAR,
    confidence FLOAT,
    citations VARCHAR
);

-- MAR Volume (monthly)
CREATE TABLE IF NOT EXISTS mar_combined_m (
    asset_class VARCHAR,
    product_type VARCHAR,
    product VARCHAR,
    year_month VARCHAR,
    year NUMBER(4,0),
    month NUMBER(2,0),
    volume DOUBLE PRECISION,
    adv DOUBLE PRECISION
);

-- -- MAR Trade Days (monthly)
-- CREATE TABLE IF NOT EXISTS mar_trade_days_m (
--     asset_class VARCHAR,
--     product VARCHAR,
--     product_type VARCHAR,
--     year_month VARCHAR,
--     year NUMBER(4,0),
--     month NUMBER(2,0),
--     trade_days DOUBLE PRECISION
-- );