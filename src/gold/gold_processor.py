from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable


def process_gold(
    spark,
    batch_id,
    silver_valid_batch,
    gold_table,
    gold_transaction_contribution_table,
    gold_control_table,
    invalid_transaction_versions=None
):
    """
    Maintain transaction-level Gold contributions and rebuild
    customer metrics for all affected customers.

    Features:
    - Sequential batch retry safety
    - Transaction-level incremental processing
    - Updated transaction handling
    - Customer reassignment
    - Soft invalidation using is_active
    - Stale transaction protection
    - Partial-failure recovery

    Concurrent executions of the same batch are not supported.
    """

    print(f"Starting Gold processing for batch: {batch_id}")

    # ============================================================
    # 1. CHECK GOLD BATCH CONTROL
    # ============================================================

    gold_already_processed = (
        spark.table(gold_control_table)
        .filter(
            (F.col("batch_id") == batch_id) &
            (F.col("status") == "APPLIED")
        )
        .limit(1)
        .count() > 0
    )

    if gold_already_processed:
        print(f"Gold already processed batch {batch_id}. Skipping.")
        return None

    # ============================================================
    # 2. PREPARE VALID TRANSACTION CONTRIBUTIONS
    # ============================================================

    contribution_source = (
        silver_valid_batch
        .filter(
            F.col("transaction_id").isNotNull() &
            F.col("customer_id").isNotNull()
        )
        .select(
            "transaction_id",
            "customer_id",
            "amount",
            "transaction_type",
            "transaction_date",
            "updated_at"
        )
        .withColumn(
            "_updated_at_ts",
            F.to_timestamp("updated_at")
        )
        .withColumn(
            "processed_at",
            F.current_timestamp()
        )
        .withColumn(
            "is_active",
            F.lit(True)
        )
    )

    # Deterministic tie-breaker within the incoming batch.
    contribution_source = contribution_source.withColumn(
        "_record_hash",
        F.sha2(
            F.to_json(
                F.struct(
                    "customer_id",
                    "amount",
                    "transaction_type",
                    "transaction_date",
                    "updated_at"
                ),
                {"ignoreNullFields": "false"}
            ),
            256
        )
    )

    source_window = (
        Window
        .partitionBy("transaction_id")
        .orderBy(
            F.col("_updated_at_ts").desc_nulls_last(),
            F.col("_record_hash").desc()
        )
    )

    contribution_source = (
        contribution_source
        .withColumn(
            "_row_num",
            F.row_number().over(source_window)
        )
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", "_record_hash")
    )

    # ============================================================
    # 3. PREPARE INVALID TRANSACTION VERSIONS
    # ============================================================

    if invalid_transaction_versions is not None:

        invalid_versions = (
            invalid_transaction_versions
            .select(
                "transaction_id",
                "updated_at"
            )
            .filter(F.col("transaction_id").isNotNull())
            .distinct()
        )

    else:

        invalid_versions = spark.createDataFrame(
            [],
            "transaction_id LONG, updated_at STRING"
        )

    # A transaction cannot be valid and invalid in the same
    # Gold execution.
    overlapping_ids = (
        contribution_source
        .select("transaction_id")
        .join(
            invalid_versions.select("transaction_id"),
            on="transaction_id",
            how="inner"
        )
    )

    if overlapping_ids.limit(1).count() > 0:
        raise ValueError(
            "A transaction ID appears in both valid and invalid "
            "Gold inputs. Resolve the Silver classification first."
        )

    # ============================================================
    # 4. IDENTIFY AFFECTED CUSTOMERS
    # ============================================================

    existing_contributions = spark.table(
        gold_transaction_contribution_table
    )

    incoming_ids = (
        contribution_source
        .select("transaction_id")
        .distinct()
    )

    changed_ids = (
        incoming_ids
        .unionByName(
            invalid_versions.select("transaction_id")
        )
        .distinct()
    )

    previous_customers = (
        existing_contributions
        .join(
            changed_ids,
            on="transaction_id",
            how="inner"
        )
        .select("customer_id")
    )

    incoming_customers = (
        contribution_source
        .select("customer_id")
    )

    affected_customers = (
        previous_customers
        .unionByName(incoming_customers)
        .filter(F.col("customer_id").isNotNull())
        .distinct()
    )

    # ============================================================
    # 5. SOFT INVALIDATION
    # ============================================================

    # Invalid transactions are retained in the contribution
    # table with is_active = false.
    #
    # Their latest updated_at timestamp is retained so that
    # older valid versions cannot be inserted later.

    invalid_versions_for_merge = (
        invalid_versions
        .withColumn(
            "customer_id",
            F.lit(None).cast("long")
        )
        .withColumn(
            "amount",
            F.lit(None).cast("double")
        )
        .withColumn(
            "transaction_type",
            F.lit(None).cast("string")
        )
        .withColumn(
            "transaction_date",
            F.lit(None).cast("string")
        )
        .withColumn(
            "processed_at",
            F.current_timestamp()
        )
        .withColumn(
            "is_active",
            F.lit(False)
        )
        .select(
            "transaction_id",
            "customer_id",
            "amount",
            "transaction_type",
            "transaction_date",
            "updated_at",
            "processed_at",
            "is_active"
        )
    )

    if invalid_versions_for_merge.limit(1).count() > 0:

        contribution_target = DeltaTable.forName(
            spark,
            gold_transaction_contribution_table
        )

        (
            contribution_target.alias("t")
            .merge(
                invalid_versions_for_merge.alias("s"),
                "t.transaction_id = s.transaction_id"
            )
            .whenMatchedUpdate(
                condition="""
                    s.updated_at IS NOT NULL
                    AND (
                        t.updated_at IS NULL
                        OR CAST(s.updated_at AS TIMESTAMP)
                           > CAST(t.updated_at AS TIMESTAMP)
                    )
                """,
                set={
                    "updated_at": "s.updated_at",
                    "processed_at": "s.processed_at",
                    "is_active": "false"
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("Invalid transaction versions recorded as inactive.")

    # ============================================================
    # 6. MERGE VALID TRANSACTION CONTRIBUTIONS
    # ============================================================

    contribution_source_for_merge = (
        contribution_source
        .drop("_updated_at_ts")
    )

    contribution_target = DeltaTable.forName(
        spark,
        gold_transaction_contribution_table
    )

    (
        contribution_target.alias("t")
        .merge(
            contribution_source_for_merge.alias("s"),
            "t.transaction_id = s.transaction_id"
        )
        .whenMatchedUpdate(
            condition="""
                s.updated_at IS NOT NULL
                AND (
                    t.updated_at IS NULL
                    OR CAST(s.updated_at AS TIMESTAMP)
                       > CAST(t.updated_at AS TIMESTAMP)
                )
            """,
            set={
                "customer_id": "s.customer_id",
                "amount": "s.amount",
                "transaction_type": "s.transaction_type",
                "transaction_date": "s.transaction_date",
                "updated_at": "s.updated_at",
                "processed_at": "s.processed_at",
                "is_active": "true"
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Transaction contribution MERGE completed.")

    # ============================================================
    # 7. REBUILD METRICS FOR AFFECTED CUSTOMERS
    # ============================================================

    remaining_contributions = (
        spark.table(gold_transaction_contribution_table)
        .filter(F.col("is_active") == True)
    )

    customer_aggregates = (
        remaining_contributions
        .join(
            affected_customers,
            on="customer_id",
            how="inner"
        )
        .groupBy("customer_id")
        .agg(
            F.count("transaction_id").alias("transaction_count"),

            F.sum("amount").alias("total_amount"),

            F.sum(
                F.when(
                    F.col("transaction_type") == "DEBIT",
                    F.col("amount")
                ).otherwise(0)
            ).alias("total_debit"),

            F.sum(
                F.when(
                    F.col("transaction_type") == "CREDIT",
                    F.col("amount")
                ).otherwise(0)
            ).alias("total_credit"),

            F.max("transaction_date").alias("latest_transaction")
        )
    )

    # Retain customers even if they have no active contributions.
    gold_rebuild = (
        affected_customers
        .join(
            customer_aggregates,
            on="customer_id",
            how="left"
        )
        .fillna(
            {
                "transaction_count": 0,
                "total_amount": 0.0,
                "total_debit": 0.0,
                "total_credit": 0.0
            }
        )
    )

    # ============================================================
    # 8. CALCULATE CUSTOMER METRICS
    # ============================================================

    gold_rebuild = (
        gold_rebuild
        .withColumn(
            "average_transaction_amount",
            F.when(
                F.col("transaction_count") > 0,
                F.round(
                    F.col("total_amount") /
                    F.col("transaction_count"),
                    2
                )
            ).otherwise(0.0)
        )
        .withColumn(
            "activity_level",
            F.when(F.col("transaction_count") >= 5, "HIGH")
            .when(F.col("transaction_count") >= 3, "MEDIUM")
            .when(F.col("transaction_count") > 0, "LOW")
            .otherwise("NONE")
        )
        .withColumn(
            "high_value_customer",
            F.when(
                F.col("average_transaction_amount") > 5000,
                "YES"
            ).otherwise("NO")
        )
    )

    # ============================================================
    # 9. MERGE CUSTOMER METRICS INTO GOLD
    # ============================================================

    gold_target = DeltaTable.forName(
        spark,
        gold_table
    )

    (
        gold_target.alias("g")
        .merge(
            gold_rebuild.alias("s"),
            "g.customer_id = s.customer_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Gold customer metrics MERGE completed.")

    # ============================================================
    # 10. RECORD SUCCESSFUL GOLD PROCESSING
    # ============================================================

    gold_control_record = (
        spark.createDataFrame(
            [(batch_id, "APPLIED")],
            ["batch_id", "status"]
        )
        .withColumn(
            "processed_at",
            F.current_timestamp()
        )
    )

    gold_control_target = DeltaTable.forName(
        spark,
        gold_control_table
    )

    (
        gold_control_target.alias("t")
        .merge(
            gold_control_record.alias("s"),
            "t.batch_id = s.batch_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"Gold batch {batch_id} successfully marked APPLIED.")

    return gold_rebuild