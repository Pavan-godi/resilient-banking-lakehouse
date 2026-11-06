-- ============================================================
-- BANKING LAKEHOUSE
-- TABLE CREATION SCRIPT
-- ============================================================

-- 1. BRONZE TRANSACTIONS

CREATE TABLE IF NOT EXISTS workspace.default.bronze_transactions (
    transaction_id BIGINT,
    account_id BIGINT,
    customer_id BIGINT,
    transaction_date STRING,
    transaction_type STRING,
    amount DOUBLE,
    updated_at STRING,
    _ingestion_timestamp TIMESTAMP,
    batch_id STRING
)
USING DELTA;


-- 2. SILVER TRANSACTIONS

CREATE TABLE IF NOT EXISTS workspace.default.silver_transactions (
    transaction_id BIGINT,
    account_id BIGINT,
    customer_id BIGINT,
    transaction_date STRING,
    transaction_type STRING,
    amount DOUBLE,
    updated_at STRING,
    _ingestion_timestamp TIMESTAMP,
    batch_id STRING
)
USING DELTA;


-- 3. QUARANTINE TRANSACTIONS

CREATE TABLE IF NOT EXISTS workspace.default.quarantine_transactions_v2 (
    quarantine_id STRING NOT NULL,
    transaction_id BIGINT,
    account_id BIGINT,
    customer_id BIGINT,
    transaction_date STRING,
    transaction_type STRING,
    amount DOUBLE,
    updated_at STRING,
    _ingestion_timestamp TIMESTAMP,
    batch_id STRING,
    dq_reason STRING
)
USING DELTA;


-- 4. GOLD CUSTOMER METRICS

CREATE TABLE IF NOT EXISTS workspace.default.gold_customer_metrics (
    customer_id BIGINT,
    transaction_count BIGINT,
    total_amount DOUBLE,
    total_debit DOUBLE,
    total_credit DOUBLE,
    latest_transaction STRING,
    activity_level STRING,
    average_transaction_amount DOUBLE,
    high_value_customer STRING
)
USING DELTA;


-- 5. GOLD TRANSACTION CONTRIBUTIONS

CREATE TABLE IF NOT EXISTS workspace.default.gold_transaction_contributions (
    transaction_id BIGINT,
    customer_id BIGINT,
    amount DOUBLE,
    transaction_type STRING,
    transaction_date STRING,
    updated_at STRING,
    processed_at TIMESTAMP,
    is_active BOOLEAN
)
USING DELTA;


-- 6. GOLD BATCH CONTROL

CREATE TABLE IF NOT EXISTS workspace.default.gold_batch_control (
    batch_id STRING,
    status STRING,
    processed_at TIMESTAMP
)
USING DELTA;


-- 7. DATA QUALITY SUMMARY

CREATE TABLE IF NOT EXISTS workspace.default.etl_dq_summary (
    batch_id STRING,
    total_records BIGINT,
    valid_records BIGINT,
    quarantine_records BIGINT,
    dq_reconciliation_status STRING,
    duplicate_records BIGINT
)
USING DELTA;


-- 8. ETL BATCH CONTROL

CREATE TABLE IF NOT EXISTS workspace.default.etl_batch_control (
    batch_id STRING,
    status STRING,
    processed_at STRING,
    error_messge STRING
)
USING DELTA;