-- ==============================
-- Query 1: Data Freshness
-- ==============================
SELECT
    MAX(created_at) AS warehouse_latest
FROM warehouse.fact_sales;


-- ==============================
-- Query 2: Volume Trend (30 Days)
-- ==============================
SELECT
    DATE(created_at) AS sale_date,
    COUNT(*) AS daily_transactions
FROM warehouse.fact_sales
WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(created_at)
ORDER BY sale_date;


-- ==============================
-- Query 3: Data Quality Issues
-- ==============================
SELECT
    COUNT(*) AS null_violations
FROM warehouse.fact_sales
WHERE customer_id IS NULL
   OR product_id IS NULL
   OR revenue IS NULL;


-- ==============================
-- Query 4: Pipeline Execution History
-- (JSON based – read from pipeline_execution_report.json)
-- ==============================
-- Handled via Python


-- ==============================
-- Query 5: Database Statistics
-- ==============================
SELECT
    relname AS table_name,
    n_live_tup AS row_count
FROM pg_stat_user_tables
ORDER BY n_live_tup DESC;
