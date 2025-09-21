-- Create logs table
CREATE TABLE IF NOT EXISTS logs (
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    question TEXT,
    confidence FLOAT,
    citations TEXT
);

-- MAR Volume (monthly)
CREATE TABLE IF NOT EXISTS mar_volume_m (
    asset_class TEXT,
    product TEXT,
    product_type TEXT,
    year_month TEXT,
    year INTEGER,
    month INTEGER,
    volume DOUBLE
);

-- MAR Trade Days (monthly)
CREATE TABLE IF NOT EXISTS mar_trade_days_m (
    asset_class TEXT,
    product TEXT,
    product_type TEXT,
    year_month TEXT,
    year INTEGER,
    month INTEGER,
    trade_days DOUBLE
);

-- -- Create pr_index table
-- CREATE TABLE IF NOT EXISTS pr_index (
--     id VARCHAR PRIMARY KEY,
--     url TEXT,
--     report_name TEXT,
--     report_type TEXT,
--     year INTEGER,
--     month INTEGER,
--     quarter INTEGER,
--     chunk_index INTEGER,
--     content TEXT,
--     emb BLOB,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );