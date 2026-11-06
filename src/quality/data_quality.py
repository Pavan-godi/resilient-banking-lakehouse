from numbers import Integral

from delta.tables import DeltaTable

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)


def reconcile_data_quality(
    spark,
    batch_id,
    bronze_count,
    valid_count,
    quarantine_count,
    duplicate_count,
    dq_table,
    force_failure=False
):
    """
    Reconcile Bronze records against Silver valid and
    Quarantine records.

    Duplicate records are included in quarantine_count.
    duplicate_count is retained as a diagnostic metric.
    """

    print(f"Starting DQ reconciliation for batch: {batch_id}")

    # 1. Validate input counts
    counts = {
        "bronze_count": bronze_count,
        "valid_count": valid_count,
        "quarantine_count": quarantine_count,
        "duplicate_count": duplicate_count
    }

    for name, value in counts.items():

        if not isinstance(value, Integral):
            raise TypeError(
                f"{name} must be an integer. Received: {type(value)}"
            )

        if value < 0:
            raise ValueError(
                f"{name} cannot be negative. Received: {value}"
            )

    # 2. Reconcile record counts
    reconciled_count = valid_count + quarantine_count

    difference_count = bronze_count - reconciled_count

    dq_status = (
        "OK"
        if difference_count == 0
        else "FAIL"
    )

    # 3. Prepare DQ result
    dq_schema = StructType([
        StructField("batch_id", StringType(), False),
        StructField("total_records", LongType(), True),
        StructField("valid_records", LongType(), True),
        StructField("quarantine_records", LongType(), True),
        StructField("duplicate_records", LongType(), True),
        StructField("dq_reconciliation_status", StringType(), True)
    ])

    dq_result = spark.createDataFrame(
        [(
            batch_id,
            int(bronze_count),
            int(valid_count),
            int(quarantine_count),
            int(duplicate_count),
            dq_status
        )],
        schema=dq_schema
    )

    # 4. Persist DQ result using batch_id as the key
    dq_target = DeltaTable.forName(spark, dq_table)

    (
        dq_target.alias("t")
        .merge(
            dq_result.alias("s"),
            "t.batch_id = s.batch_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    # 5. Optional failure injection for retry testing
    if force_failure:
        raise RuntimeError(
            "Forced failure after DQ result was written."
        )

    # 6. Print reconciliation summary
    print("\nDATA QUALITY SUMMARY")
    print(f"Bronze records:       {bronze_count}")
    print(f"Valid records:        {valid_count}")
    print(f"Quarantine records:   {quarantine_count}")
    print(f"Duplicate records:    {duplicate_count}")
    print(f"Reconciled records:   {reconciled_count}")
    print(f"Difference:           {difference_count}")
    print(f"DQ status:            {dq_status}")

    if dq_status != "OK":
        raise RuntimeError(
            "Data quality reconciliation failed. "
            f"Bronze={bronze_count}, "
            f"Valid={valid_count}, "
            f"Quarantine={quarantine_count}, "
            f"Difference={difference_count}"
        )

    print("Data quality reconciliation completed.")

    return dq_result