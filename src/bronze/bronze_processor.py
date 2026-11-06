
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def ingest_to_bronze(spark, incoming_df, batch_id, bronze_table):
    """
    Ingest source records into the Bronze Delta table.

    Uses a deterministic source-record identifier for
    sequential retry safety.

    Preserves duplicate source records while preventing
    the same records from being inserted again on retry.

    Concurrent executions of the same batch are not supported.
    """

    print(f"Starting Bronze ingestion for batch: {batch_id}")

    source_columns = incoming_df.columns

    # ============================================================
    # 1. GENERATE DETERMINISTIC SOURCE RECORD IDENTIFIERS
    # ============================================================

    def add_source_record_id(df):

        payload = F.to_json(
            F.struct(
                *[
                    F.col(column)
                    for column in source_columns
                ]
            ),
            {"ignoreNullFields": "false"}
        )

        df = df.withColumn(
            "_record_payload",
            payload
        )

        occurrence_window = (
            Window
            .partitionBy("_record_payload")
            .orderBy(F.lit(1))
        )

        df = df.withColumn(
            "_duplicate_occurrence",
            F.row_number().over(occurrence_window)
        )

        df = df.withColumn(
            "_source_record_id",
            F.sha2(
                F.to_json(
                    F.struct(
                        F.lit(batch_id).alias("batch_id"),
                        F.col("_record_payload").alias("payload"),
                        F.col("_duplicate_occurrence").alias("occurrence")
                    ),
                    {"ignoreNullFields": "false"}
                ),
                256
            )
        )

        return df.drop(
            "_record_payload",
            "_duplicate_occurrence"
        )

    # ============================================================
    # 2. PREPARE INCOMING RECORDS
    # ============================================================

    incoming_with_id = add_source_record_id(
        incoming_df
    )

    bronze_batch_df = (
        incoming_with_id
        .withColumn(
            "batch_id",
            F.lit(batch_id)
        )
        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp()
        )
    )

    expected_count = incoming_df.count()

    # ============================================================
    # 3. READ EXISTING RECORDS FOR THE SAME BATCH
    # ============================================================

    existing_bronze = (
        spark.table(bronze_table)
        .filter(
            F.col("batch_id") == batch_id
        )
        .select(*source_columns)
    )

    existing_with_id = add_source_record_id(
        existing_bronze
    )

    existing_record_ids = (
        existing_with_id
        .select("_source_record_id")
        .distinct()
    )

    # ============================================================
    # 4. IDENTIFY RECORDS NOT ALREADY INGESTED
    # ============================================================

    bronze_to_write = (
        bronze_batch_df
        .join(
            existing_record_ids,
            on="_source_record_id",
            how="left_anti"
        )
        .drop("_source_record_id")
    )

    records_to_write = bronze_to_write.count()

    print(f"Incoming records: {expected_count}")
    print(f"Records to write: {records_to_write}")
    print(
        f"Records already present: "
        f"{expected_count - records_to_write}"
    )

    # ============================================================
    # 5. WRITE NEW RECORDS
    # ============================================================

    if records_to_write > 0:

        (
            bronze_to_write.write
            .format("delta")
            .mode("append")
            .saveAsTable(bronze_table)
        )

        print("Bronze ingestion completed.")

    else:

        print("All incoming records already exist in Bronze.")

    # ============================================================
    # 6. RETURN CURRENT BATCH
    # ============================================================

    bronze_current_batch = (
        spark.table(bronze_table)
        .filter(
            F.col("batch_id") == batch_id
        )
    )

    bronze_count = bronze_current_batch.count()

    print(
        f"Total Bronze records for this batch: {bronze_count}"
    )

    return bronze_current_batch
