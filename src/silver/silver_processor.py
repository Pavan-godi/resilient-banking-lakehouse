from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable


def process_silver(
    spark,
    bronze_current_batch,
    silver_table,
    quarantine_table
):
    """
    Validate and process Bronze transactions.

    Features:
    - Latest-version selection
    - Duplicate preservation in Quarantine
    - NULL transaction ID handling
    - Deterministic quarantine identifiers
    - Silver upsert and invalidation
    - Gold invalid transaction tracking
    - DQ-compatible record counts

    Duplicate records are preserved in Quarantine.
    Concurrent executions of the same batch are not supported.
    """

    print("Starting Silver processing...")

    # ============================================================
    # 1. DEFINE SOURCE IDENTITY AND VALIDATION RULES
    # ============================================================

    identity_columns = [
        "transaction_id",
        "account_id",
        "customer_id",
        "transaction_date",
        "transaction_type",
        "amount",
        "updated_at",
        "batch_id"
    ]

    valid_condition = (
        F.col("transaction_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("amount").isNotNull()
        & (F.col("amount") >= 0)
        & F.col("transaction_type").isin("DEBIT", "CREDIT")
    )

    # Preserve the original Bronze ingestion timestamp.
    source_df = bronze_current_batch

    # ============================================================
    # 2. GENERATE DETERMINISTIC RECORD FINGERPRINTS
    # ============================================================

    source_df = source_df.withColumn(
        "_record_payload",
        F.to_json(
            F.struct(
                *[
                    F.col(column)
                    for column in identity_columns
                ]
            ),
            {"ignoreNullFields": "false"}
        )
    )

    occurrence_window = (
        Window
        .partitionBy("_record_payload")
        .orderBy(F.lit(1))
    )

    source_df = source_df.withColumn(
        "_duplicate_occurrence",
        F.row_number().over(occurrence_window)
    )

    source_df = source_df.withColumn(
        "_source_record_id",
        F.sha2(
            F.to_json(
                F.struct(
                    F.col("batch_id"),
                    F.col("_record_payload"),
                    F.col("_duplicate_occurrence")
                ),
                {"ignoreNullFields": "false"}
            ),
            256
        )
    )

    # ============================================================
    # 3. IDENTIFY LATEST TRANSACTION VERSIONS
    # ============================================================

    source_df = source_df.withColumn(
        "_updated_at_ts",
        F.to_timestamp("updated_at")
    )

    transaction_window = (
        Window
        .partitionBy("transaction_id")
        .orderBy(
            F.col("_updated_at_ts").desc_nulls_last(),
            F.col("_record_payload").desc(),
            F.col("_duplicate_occurrence").asc()
        )
    )

    # NULL transaction IDs are not grouped together.
    non_null_transactions = source_df.filter(
        F.col("transaction_id").isNotNull()
    )

    ranked_transactions = (
        non_null_transactions
        .withColumn(
            "_row_num",
            F.row_number().over(transaction_window)
        )
    )

    latest_transactions = (
        ranked_transactions
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    duplicate_transactions = (
        ranked_transactions
        .filter(F.col("_row_num") > 1)
        .drop("_row_num")
    )

    # ============================================================
    # 4. SEPARATE VALID AND INVALID LATEST RECORDS
    # ============================================================

    silver_valid_batch = (
        latest_transactions
        .filter(valid_condition)
        .drop(
            "_record_payload",
            "_duplicate_occurrence",
            "_source_record_id",
            "_updated_at_ts"
        )
    )

    latest_invalid = (
        latest_transactions
        .filter(~F.coalesce(valid_condition, F.lit(False)))
    )

    null_id_invalid = (
        source_df
        .filter(F.col("transaction_id").isNull())
    )

    # ============================================================
    # 5. PREPARE QUARANTINE RECORDS
    # ============================================================

    # Superseded versions are preserved as duplicates.
    duplicate_quarantine = (
        duplicate_transactions
        .withColumn(
            "dq_reason",
            F.lit("DUPLICATE_TRANSACTION")
        )
    )

    # Latest invalid transaction versions retain their
    # actual validation failure reason.
    invalid_quarantine = (
        latest_invalid
        .withColumn(
            "dq_reason",
            F.when(
                F.col("customer_id").isNull(),
                "MISSING_CUSTOMER_ID"
            )
            .when(
                F.col("amount").isNull() |
                (F.col("amount") < 0),
                "INVALID_AMOUNT"
            )
            .when(
                F.col("transaction_type").isNull() |
                (~F.col("transaction_type").isin("DEBIT", "CREDIT")),
                "INVALID_TRANSACTION_TYPE"
            )
            .otherwise("UNKNOWN")
        )
    )

    # NULL transaction IDs are preserved individually.
    null_id_quarantine = (
        null_id_invalid
        .withColumn(
            "dq_reason",
            F.lit("MISSING_TRANSACTION_ID")
        )
    )

    quarantine_batch = (
        duplicate_quarantine
        .unionByName(invalid_quarantine)
        .unionByName(null_id_quarantine)
    )

    # Use the source-record fingerprint as the quarantine key.
    quarantine_batch = (
        quarantine_batch
        .withColumn(
            "quarantine_id",
            F.col("_source_record_id")
        )
        .select(
            "quarantine_id",
            "transaction_id",
            "account_id",
            "customer_id",
            "transaction_date",
            "transaction_type",
            "amount",
            "updated_at",
            "_ingestion_timestamp",
            "batch_id",
            "dq_reason"
        )
    )

    # ============================================================
    # 6. COUNT RECORDS AND DUPLICATES
    # ============================================================

    valid_count = silver_valid_batch.count()

    quarantine_count = quarantine_batch.count()

    duplicate_count = (
        quarantine_batch
        .filter(
            F.col("dq_reason") == "DUPLICATE_TRANSACTION"
        )
        .count()
    )

    bronze_count = bronze_current_batch.count()

    # Every source record must belong to exactly one output.
    reconciled_count = valid_count + quarantine_count

    if reconciled_count != bronze_count:
        raise RuntimeError(
            "Silver record reconciliation failed. "
            f"Bronze={bronze_count}, "
            f"Valid={valid_count}, "
            f"Quarantine={quarantine_count}"
        )

    # ============================================================
    # 7. IDENTIFY INVALIDATED TRANSACTIONS
    # ============================================================

    invalid_ids = (
        latest_invalid
        .select(
            "transaction_id",
            "updated_at",
            "_updated_at_ts"
        )
        .distinct()
    )

    existing_silver = spark.table(silver_table).alias("t")

    incoming_invalid = invalid_ids.alias("s")

    invalid_comparison = (
        incoming_invalid
        .join(
            existing_silver,
            F.col("s.transaction_id") ==
            F.col("t.transaction_id"),
            "left"
        )
    )

    invalid_transaction_versions = (
    invalid_comparison
    .filter(
        F.col("t.transaction_id").isNull()
        | (
            F.col("s._updated_at_ts").isNotNull()
            & (
                F.col("t.updated_at").isNull()
                | (
                    F.col("s._updated_at_ts")
                    > F.to_timestamp(F.col("t.updated_at"))
                )
            )
        )
    )
    .select(
        F.col("s.transaction_id").alias("transaction_id"),
        F.col("s.updated_at").alias("updated_at")
    )
    .distinct()
)
    # ============================================================
    # 8. REMOVE INVALIDATED TRANSACTIONS FROM SILVER
    # ============================================================

    if invalid_transaction_versions.limit(1).count() > 0:

        silver_target = DeltaTable.forName(
            spark,
            silver_table
        )

        (
            silver_target.alias("t")
            .merge(
                invalid_transaction_versions.alias("s"),
                "t.transaction_id = s.transaction_id"
            )
            .whenMatchedDelete()
            .execute()
        )

        print("Invalidated transactions removed from Silver.")

    # ============================================================
    # 9. MERGE VALID RECORDS INTO SILVER
    # ============================================================

    silver_target = DeltaTable.forName(
        spark,
        silver_table
    )

    (
        silver_target.alias("t")
        .merge(
            silver_valid_batch.alias("s"),
            "t.transaction_id = s.transaction_id"
        )
        .whenMatchedUpdateAll(
            condition="""
                CAST(s.updated_at AS TIMESTAMP) IS NOT NULL
                AND (
                    t.updated_at IS NULL
                    OR CAST(s.updated_at AS TIMESTAMP)
                       > CAST(t.updated_at AS TIMESTAMP)
                )
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Silver MERGE completed.")

    # ============================================================
    # 10. MERGE QUARANTINE RECORDS
    # ============================================================

    quarantine_target = DeltaTable.forName(
        spark,
        quarantine_table
    )

    (
        quarantine_target.alias("t")
        .merge(
            quarantine_batch.alias("s"),
            "t.quarantine_id = s.quarantine_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Quarantine MERGE completed.")

    # ============================================================
    # 11. RETURN PROCESSING RESULTS
    # ============================================================

    print("Silver processing completed.")
    print(f"Bronze records: {bronze_count}")
    print(f"Valid records: {valid_count}")
    print(f"Quarantined records: {quarantine_count}")
    print(f"Duplicate records: {duplicate_count}")

    return {
        "silver_valid_batch": silver_valid_batch,
        "quarantine_batch": quarantine_batch,
        "invalid_transaction_versions": invalid_transaction_versions,
        "bronze_count": bronze_count,
        "valid_count": valid_count,
        "quarantine_count": quarantine_count,
        "duplicate_count": duplicate_count
    }

