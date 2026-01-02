-- =====================================================
-- DATA QUALITY CHECKS
-- Schema: staging
-- Each query MUST return: (check_name, violation_count)
-- =====================================================

-- -------------------------
-- 1. COMPLETENESS CHECKS
-- -------------------------

-- Customers mandatory fields
SELECT
    'customers.mandatory_nulls' AS check_name,
    COUNT(*) AS violation_count
FROM staging.customers
WHERE
    customer_id IS NULL
    OR email IS NULL
    OR first_name IS NULL
    OR last_name IS NULL;

-- Products mandatory fields
SELECT
    'products.mandatory_nulls' AS check_name,
    COUNT(*) AS violation_count
FROM staging.products
WHERE
    product_id IS NULL
    OR product_name IS NULL
    OR price IS NULL
    OR cost IS NULL;

-- Transactions mandatory fields
SELECT
    'transactions.mandatory_nulls' AS check_name,
    COUNT(*) AS violation_count
FROM staging.transactions
WHERE
    transaction_id IS NULL
    OR customer_id IS NULL
    OR total_amount IS NULL;

-- -------------------------
-- 2. UNIQUENESS CHECKS
-- -------------------------

-- Duplicate customer emails
SELECT
    'duplicate_emails' AS check_name,
    COUNT(*) AS violation_count
FROM (
    SELECT email
    FROM staging.customers
    GROUP BY email
    HAVING COUNT(*) > 1
) t;

-- Duplicate transactions (same customer, date, time, amount)
SELECT
    'duplicate_transactions' AS check_name,
    COUNT(*) AS violation_count
FROM (
    SELECT customer_id, transaction_date, transaction_time, total_amount
    FROM staging.transactions
    GROUP BY customer_id, transaction_date, transaction_time, total_amount
    HAVING COUNT(*) > 1
) t;

-- -------------------------
-- 3. REFERENTIAL INTEGRITY
-- -------------------------

-- Orphan transactions (customer missing)
SELECT
    'orphan_transactions' AS check_name,
    COUNT(*) AS violation_count
FROM staging.transactions t
LEFT JOIN staging.customers c
    ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Orphan transaction_items (transaction missing)
SELECT
    'orphan_items_transaction' AS check_name,
    COUNT(*) AS violation_count
FROM staging.transaction_items ti
LEFT JOIN staging.transactions t
    ON ti.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

-- Orphan transaction_items (product missing)
SELECT
    'orphan_items_product' AS check_name,
    COUNT(*) AS violation_count
FROM staging.transaction_items ti
LEFT JOIN staging.products p
    ON ti.product_id = p.product_id
WHERE p.product_id IS NULL;

-- -------------------------
-- 4. RANGE / VALIDITY CHECKS
-- -------------------------

SELECT
    'range_violations' AS check_name,
    COUNT(*) AS violation_count
FROM staging.transaction_items
WHERE
    quantity <= 0
    OR unit_price <= 0
    OR discount_percentage < 0
    OR discount_percentage > 100;

-- -------------------------
-- 5. CONSISTENCY CHECKS
-- -------------------------

-- Line total mismatch
SELECT
    'line_total_mismatch' AS check_name,
    COUNT(*) AS violation_count
FROM staging.transaction_items
WHERE
    ABS(
        line_total -
        (quantity * unit_price * (1 - discount_percentage / 100.0))
    ) > 0.01;

-- Product cost vs price
SELECT
    'product_cost_price_violation' AS check_name,
    COUNT(*) AS violation_count
FROM staging.products
WHERE cost >= price;

-- Transaction total mismatch
SELECT
    'transaction_total_mismatch' AS check_name,
    COUNT(*) AS violation_count
FROM staging.transactions t
JOIN (
    SELECT
        transaction_id,
        SUM(line_total) AS item_total
    FROM staging.transaction_items
    GROUP BY transaction_id
) i
ON t.transaction_id = i.transaction_id
WHERE ABS(t.total_amount - i.item_total) > 0.01;

-- -------------------------
-- 6. ACCURACY / BUSINESS RULES
-- -------------------------

-- Future transaction dates
SELECT
    'future_transactions' AS check_name,
    COUNT(*) AS violation_count
FROM staging.transactions
WHERE transaction_date > CURRENT_DATE;

-- Customer registration after transaction
SELECT
    'registration_after_transaction' AS check_name,
    COUNT(*) AS violation_count
FROM staging.transactions t
JOIN staging.customers c
    ON t.customer_id = c.customer_id
WHERE c.registration_date > t.transaction_date;
