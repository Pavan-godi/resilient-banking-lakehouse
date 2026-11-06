-- 1. PIPELINE BATCH STATUS
SELECT
    batch_id,
    status,
    processed_at,
    error_messge
FROM workspace.default.etl_batch_control
ORDER BY processed_at DESC;


-- 2. DATA QUALITY RECONCILIATION
SELECT
    batch_id,
    total_records,
    valid_records,
    quarantine_records,
    duplicate_records,
    dq_reconciliation_status
FROM workspace.default.etl_dq_summary
ORDER BY batch_id;


-- 3. SILVER LAYER RECORD COUNTS
SELECT
    COUNT(*) AS silver_record_count,
    COUNT(DISTINCT transaction_id) AS unique_transaction_count
FROM workspace.default.silver_transactions;


-- 4. QUARANTINED RECORDS BY REASON
SELECT
    dq_reason,
    COUNT(*) AS rejected_record_count
FROM workspace.default.quarantine_transactions_v2
GROUP BY dq_reason
ORDER BY rejected_record_count DESC;


-- 5. GOLD BATCH APPLICATION STATUS
SELECT
    batch_id,
    status,
    processed_at
FROM workspace.default.gold_batch_control
ORDER BY processed_at DESC;


-- 6. GOLD CONTRIBUTION VALIDATION
SELECT
    COUNT(*) AS total_contributions,
    SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) AS active_contributions,
    SUM(CASE WHEN is_active = FALSE THEN 1 ELSE 0 END) AS inactive_contributions
FROM workspace.default.gold_transaction_contributions;