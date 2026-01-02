-- =========================================================
-- QUERY 1: Top 10 Products by Revenue
-- Objective: Identify best-selling products by revenue
-- Business Question: Which products generate the highest revenue?
-- =========================================================

SELECT
    p.product_name,
    p.category,
    SUM(f.revenue) AS total_revenue,
    SUM(f.quantity) AS units_sold,
    ROUND(SUM(f.revenue) / NULLIF(SUM(f.quantity), 0), 2) AS avg_price
FROM warehouse.fact_sales f
JOIN warehouse.dim_products p
    ON f.product_id = p.product_id
WHERE p.is_current = TRUE
GROUP BY
    p.product_name,
    p.category
ORDER BY total_revenue DESC
LIMIT 10;




-- =========================================================
-- QUERY 2: Monthly Sales Trend
-- Objective: Analyze revenue trends over time (monthly)
-- Business Question: How do sales perform month over month?
-- =========================================================

SELECT
    CONCAT(d.year, '-', LPAD(d.month::TEXT, 2, '0')) AS year_month,
    SUM(f.revenue) AS total_revenue,
    COUNT(DISTINCT f.transaction_id) AS total_transactions,
    ROUND(
        SUM(f.revenue) / NULLIF(COUNT(DISTINCT f.transaction_id), 0),
        2
    ) AS average_order_value,
    COUNT(DISTINCT f.customer_id) AS unique_customers
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d
    ON f.date = d.date
GROUP BY
    d.year,
    d.month
ORDER BY
    d.year,
    d.month;



-- =========================================================
-- QUERY 3: Customer Segmentation Analysis
-- Objective: Segment customers based on total spending
-- Business Question: How many customers fall into each
-- spending bucket and how valuable are they?
-- =========================================================
WITH customer_totals AS (
    SELECT
        c.customer_id,
        SUM(f.revenue) AS total_spent,
        COUNT(DISTINCT f.transaction_id) AS transaction_count
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_customers c
        ON f.customer_id = c.customer_id
    WHERE c.is_current = TRUE
    GROUP BY
        c.customer_id
)

SELECT
    CASE
        WHEN total_spent < 1000 THEN '$0-$1,000'
        WHEN total_spent BETWEEN 1000 AND 4999.99 THEN '$1,000-$5,000'
        WHEN total_spent BETWEEN 5000 AND 9999.99 THEN '$5,000-$10,000'
        ELSE '$10,000+'
    END AS spending_segment,
    COUNT(*) AS customer_count,
    ROUND(SUM(total_spent), 2) AS total_revenue,
    ROUND(
        AVG(total_spent / NULLIF(transaction_count, 0)),
        2
    ) AS avg_transaction_value
FROM customer_totals
GROUP BY spending_segment
ORDER BY
    MIN(total_spent);

-- =========================================================
-- QUERY 4: Category Performance
-- Objective: Compare revenue and profitability by category
-- =========================================================

SELECT
    p.category,
    ROUND(SUM(f.revenue), 2) AS total_revenue,
    ROUND(SUM(f.revenue - f.cost), 2) AS total_profit,
    ROUND(
        (SUM(f.revenue - f.cost) / NULLIF(SUM(f.revenue), 0)) * 100,
        2
    ) AS profit_margin_pct,
    SUM(f.quantity) AS units_sold
FROM warehouse.fact_sales f
JOIN warehouse.dim_products p
    ON f.product_id = p.product_id
WHERE p.is_current = TRUE
GROUP BY p.category
ORDER BY total_revenue DESC;


-- =========================================================
-- QUERY 5: Payment Method Distribution
-- Objective: Understand payment preferences
-- =========================================================

SELECT
    pm.payment_method_name AS payment_method,
    COUNT(DISTINCT f.transaction_id) AS transaction_count,
    ROUND(SUM(f.revenue), 2) AS total_revenue,
    ROUND(
        COUNT(DISTINCT f.transaction_id) * 100.0 /
        SUM(COUNT(DISTINCT f.transaction_id)) OVER (),
        2
    ) AS pct_of_transactions,
    ROUND(
        SUM(f.revenue) * 100.0 /
        SUM(SUM(f.revenue)) OVER (),
        2
    ) AS pct_of_revenue
FROM warehouse.fact_sales f
JOIN production.transactions t
    ON f.transaction_id = t.transaction_id
JOIN warehouse.dim_payment_method pm
    ON t.payment_method = pm.payment_method_name
GROUP BY pm.payment_method_name
ORDER BY transaction_count DESC;


-- =========================================================
-- QUERY 6: Geographic Revenue Analysis
-- Objective: Identify high-revenue states
-- =========================================================

SELECT
    c.state,
    ROUND(SUM(f.revenue), 2) AS total_revenue,
    COUNT(DISTINCT c.customer_id) AS total_customers,
    ROUND(
        SUM(f.revenue) / NULLIF(COUNT(DISTINCT c.customer_id), 0),
        2
    ) AS avg_revenue_per_customer
FROM warehouse.fact_sales f
JOIN warehouse.dim_customers c
    ON f.customer_id = c.customer_id
WHERE c.is_current = TRUE
GROUP BY c.state
ORDER BY total_revenue DESC;


-- ====================================================
-- =========================================================
-- QUERY 7: Customer Lifetime Value (CLV)
-- Objective: Analyze customer value and tenure
-- =========================================================

SELECT
    c.customer_id,
    c.full_name,
    ROUND(SUM(f.revenue), 2) AS total_spent,
    COUNT(DISTINCT f.transaction_id) AS transaction_count,
    (CURRENT_DATE - c.registration_date) AS days_since_registration,
    ROUND(
        SUM(f.revenue) / NULLIF(COUNT(DISTINCT f.transaction_id), 0),
        2
    ) AS avg_order_value
FROM warehouse.fact_sales f
JOIN warehouse.dim_customers c
    ON f.customer_id = c.customer_id
WHERE c.is_current = TRUE
GROUP BY
    c.customer_id,
    c.full_name,
    c.registration_date
ORDER BY total_spent DESC;

-- =========================================================
-- QUERY 8: Product Profitability Analysis
-- Objective: Identify most profitable products
-- =========================================================

SELECT
    p.product_name,
    p.category,
    ROUND(SUM(f.revenue - f.cost), 2) AS total_profit,
    ROUND(
        (SUM(f.revenue - f.cost) / NULLIF(SUM(f.revenue), 0)) * 100,
        2
    ) AS profit_margin,
    ROUND(SUM(f.revenue), 2) AS revenue,
    SUM(f.quantity) AS units_sold
FROM warehouse.fact_sales f
JOIN warehouse.dim_products p
    ON f.product_id = p.product_id
WHERE p.is_current = TRUE
GROUP BY
    p.product_name,
    p.category
ORDER BY total_profit DESC
LIMIT 10;


-- =========================================================
-- QUERY 9: Day of Week Sales Pattern
-- Objective: Identify temporal sales patterns
-- =========================================================

SELECT
    TO_CHAR(f.date, 'Day') AS day_name,
    ROUND(AVG(f.revenue), 2) AS avg_daily_revenue,
    ROUND(AVG(f.quantity), 2) AS avg_daily_quantity,
    ROUND(SUM(f.revenue), 2) AS total_revenue
FROM warehouse.fact_sales f
GROUP BY
    TO_CHAR(f.date, 'Day')
ORDER BY total_revenue DESC;


-- =========================================================
-- QUERY 10: Discount Impact Analysis
-- Objective: Analyze discount effectiveness
-- =========================================================

SELECT
    f.transaction_id,
    f.customer_id,
    f.product_id,
    f.date,
    f.quantity,
    ROUND(f.revenue, 2) AS revenue,
    ROUND(f.revenue - f.cost, 2) AS profit
FROM warehouse.fact_sales f
ORDER BY f.revenue DESC
LIMIT 10;


