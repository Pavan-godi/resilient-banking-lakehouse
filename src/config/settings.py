# ============================================================
# BANKING LAKEHOUSE - PIPELINE CONFIGURATION
# ============================================================

# Pipeline details
PIPELINE_NAME = "banking_end_to_end"


# ============================================================
# DELTA TABLES
# ============================================================

BRONZE_TABLE = "workspace.default.bronze_transactions"

SILVER_TABLE = "workspace.default.silver_transactions"

QUARANTINE_TABLE = "workspace.default.quarantine_transactions_v2"

GOLD_TABLE = "workspace.default.gold_customer_metrics"

GOLD_CONTRIBUTION_TABLE = (
    "workspace.default.gold_customer_contributions"
)

GOLD_TRANSACTION_CONTRIBUTION_TABLE = (
    "workspace.default.gold_transaction_contributions"
)

GOLD_CONTROL_TABLE = "workspace.default.gold_batch_control"

DQ_TABLE = "workspace.default.etl_dq_summary"

CONTROL_TABLE = "workspace.default.etl_batch_control"


# ============================================================
# PIPELINE TEST CONFIGURATION
# ============================================================

FORCE_DQ_RETRY_FAILURE = False