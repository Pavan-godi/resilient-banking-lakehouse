# ============================================================
# BANKING LAKEHOUSE - SAMPLE SOURCE DATA
# ============================================================

def create_sample_transactions(spark):
    """
    Create a controlled transaction dataset for pipeline testing.

    Includes:
    - 2 valid transactions
    - 1 transaction with a negative amount
    - 1 transaction with a missing customer_id
    - 1 transaction with an invalid transaction_type
    """

    source_data = [
        (1031, 530, 330, "2026-10-04 10:10:00",
         "DEBIT", 2500.0, "2026-10-04 10:10:00"),

        (1032, 531, 331, "2026-10-04 10:20:00",
         "CREDIT", 4000.0, "2026-10-04 10:20:00"),

        (1033, 532, 332, "2026-10-04 10:30:00",
         "DEBIT", -500.0, "2026-10-04 10:30:00"),

        (1034, 533, None, "2026-10-04 10:40:00",
         "DEBIT", 1500.0, "2026-10-04 10:40:00"),

        (1035, 534, 333, "2026-10-04 10:50:00",
         "INVALID", 2000.0, "2026-10-04 10:50:00")
    ]

    source_columns = [
        "transaction_id",
        "account_id",
        "customer_id",
        "transaction_date",
        "transaction_type",
        "amount",
        "updated_at"
    ]

    incoming_df = spark.createDataFrame(
        source_data,
        source_columns
    )

    return incoming_df