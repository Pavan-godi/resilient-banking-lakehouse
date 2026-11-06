-- ============================================================
-- BANKING LAKEHOUSE
-- CUSTOMER TRANSACTION ANALYTICS
-- ============================================================


-- 1. CUSTOMER TRANSACTION SUMMARY

SELECT
    customer_id,
    transaction_count,
    total_amount,
    total_debit,
    total_credit,
    average_transaction_amount,
    latest_transaction,
    activity_level,
    high_value_customer
FROM workspace.default.gold_customer_metrics
ORDER BY total_amount DESC;


-- 2. HIGH-VALUE CUSTOMERS

SELECT
    customer_id,
    transaction_count,
    total_amount,
    average_transaction_amount
FROM workspace.default.gold_customer_metrics
WHERE high_value_customer = 'YES'
ORDER BY total_amount DESC;


-- 3. CUSTOMER ACTIVITY DISTRIBUTION

SELECT
    activity_level,
    COUNT(*) AS customer_count
FROM workspace.default.gold_customer_metrics
GROUP BY activity_level
ORDER BY customer_count DESC;


-- 4. DEBIT AND CREDIT SUMMARY

SELECT
    SUM(total_debit) AS total_debit_amount,
    SUM(total_credit) AS total_credit_amount,
    SUM(total_amount) AS total_transaction_amount
FROM workspace.default.gold_customer_metrics;


-- 5. ACTIVE TRANSACTION CONTRIBUTIONS

SELECT
    transaction_type,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS average_amount
FROM workspace.default.gold_transaction_contributions
WHERE is_active = TRUE
GROUP BY transaction_type;