-- ============================================================
-- PostgreSQL Setup Script for E-commerce Events Pipeline
-- ============================================================
-- This script creates the database schema for storing real-time
-- e-commerce user events from the Spark Streaming pipeline.
-- ============================================================

-- Create the user_events table
CREATE TABLE IF NOT EXISTS user_events (
    -- Primary key: unique identifier for each event
    id SERIAL PRIMARY KEY,
    
    -- Event identifier (used for deduplication)
    event_id VARCHAR(50) UNIQUE NOT NULL,
    
    -- User information
    user_id VARCHAR(50) NOT NULL,
    
    -- Product information
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    
    -- Event details
    event_type VARCHAR(20) NOT NULL CHECK (event_type IN ('view', 'add_to_cart', 'purchase')),
    
    -- Timestamp when the event occurred
    event_timestamp TIMESTAMP NOT NULL,
    
    -- Metadata: when the record was inserted into the database
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- Indexes for Query Performance
-- ============================================================

-- Index on user_id for user activity queries
CREATE INDEX IF NOT EXISTS idx_user_events_user_id ON user_events(user_id);

-- Index on event_type for filtering by action
CREATE INDEX IF NOT EXISTS idx_user_events_event_type ON user_events(event_type);

-- Index on event_timestamp for time-based queries
CREATE INDEX IF NOT EXISTS idx_user_events_timestamp ON user_events(event_timestamp);

-- Index on product_id for product analytics
CREATE INDEX IF NOT EXISTS idx_user_events_product_id ON user_events(product_id);

-- Composite index for common query patterns
CREATE INDEX IF NOT EXISTS idx_user_events_type_timestamp 
    ON user_events(event_type, event_timestamp);

-- ============================================================
-- Sample Queries for Verification (commented out)
-- ============================================================

-- Count total events:
-- SELECT COUNT(*) FROM user_events;

-- Events by type:
-- SELECT event_type, COUNT(*) FROM user_events GROUP BY event_type;

-- Recent events:
-- SELECT * FROM user_events ORDER BY event_timestamp DESC LIMIT 10;

-- Events per minute (for throughput analysis):
-- SELECT 
--     DATE_TRUNC('minute', event_timestamp) as minute,
--     COUNT(*) as event_count
-- FROM user_events
-- GROUP BY DATE_TRUNC('minute', event_timestamp)
-- ORDER BY minute DESC;

-- ============================================================
-- Grant statements (if needed for different users)
-- ============================================================

-- GRANT ALL PRIVILEGES ON TABLE user_events TO your_app_user;
-- GRANT USAGE, SELECT ON SEQUENCE user_events_id_seq TO your_app_user;

COMMENT ON TABLE user_events IS 'Stores real-time e-commerce user events from Spark Streaming pipeline';
COMMENT ON COLUMN user_events.event_id IS 'Unique identifier for deduplication';
COMMENT ON COLUMN user_events.event_type IS 'Type of user action: view, add_to_cart, or purchase';

-- ============================================================
-- Views for Simplified Reporting
-- ============================================================
-- These views encapsulate common queries for analytics and dashboards.
-- Query them directly instead of writing complex SQL each time.

-- 1. Event Summary by Type
-- Usage: SELECT * FROM v_event_summary;
CREATE OR REPLACE VIEW v_event_summary AS
SELECT 
    event_type,
    COUNT(*) AS total_events,
    COUNT(DISTINCT user_id) AS unique_users,
    ROUND(AVG(price)::numeric, 2) AS avg_price,
    ROUND(SUM(price)::numeric, 2) AS total_value
FROM user_events
GROUP BY event_type;

-- 2. Hourly Event Trends (for time-series dashboards)
-- Usage: SELECT * FROM v_hourly_events WHERE event_hour > NOW() - INTERVAL '24 hours';
CREATE OR REPLACE VIEW v_hourly_events AS
SELECT 
    DATE_TRUNC('hour', event_timestamp) AS event_hour,
    event_type,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_id) AS unique_users
FROM user_events
GROUP BY DATE_TRUNC('hour', event_timestamp), event_type
ORDER BY event_hour DESC;

-- 3. Purchase Funnel (user journey analysis)
-- Usage: SELECT * FROM v_purchase_funnel WHERE purchases > 0;
CREATE OR REPLACE VIEW v_purchase_funnel AS
SELECT 
    user_id,
    COUNT(*) FILTER (WHERE event_type = 'view') AS views,
    COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS cart_adds,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases
FROM user_events
GROUP BY user_id;

-- 4. Top Products by Revenue
-- Usage: SELECT * FROM v_top_products LIMIT 10;
CREATE OR REPLACE VIEW v_top_products AS
SELECT 
    product_id,
    product_name,
    category,
    COUNT(*) AS purchase_count,
    ROUND(SUM(price)::numeric, 2) AS total_revenue
FROM user_events
WHERE event_type = 'purchase'
GROUP BY product_id, product_name, category
ORDER BY total_revenue DESC;

-- 5. Recent Activity (last 100 events)
-- Usage: SELECT * FROM v_recent_activity;
CREATE OR REPLACE VIEW v_recent_activity AS
SELECT 
    event_id,
    user_id,
    product_name,
    event_type,
    price,
    event_timestamp
FROM user_events
ORDER BY event_timestamp DESC
LIMIT 100;
