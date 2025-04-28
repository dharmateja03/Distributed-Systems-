-- dashboard_schema.sql

-- Table for window aggregations
CREATE TABLE IF NOT EXISTS market_window_aggs (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    event_count BIGINT NOT NULL,
    total_volume BIGINT,
    avg_price NUMERIC(10, 2),
    max_price NUMERIC(10, 2),
    min_price NUMERIC(10, 2),
    watermark_delay VARCHAR(30),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for performance metrics
CREATE TABLE IF NOT EXISTS window_performance (
    id SERIAL PRIMARY KEY,
    window_type VARCHAR(50) NOT NULL,
    processing_time NUMERIC(10, 4) NOT NULL,
    event_count BIGINT NOT NULL,
    memory_usage NUMERIC(10, 2),
    run_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster querying
CREATE INDEX IF NOT EXISTS idx_window_start ON market_window_aggs(window_start);
CREATE INDEX IF NOT EXISTS idx_window_type ON window_performance(window_type);