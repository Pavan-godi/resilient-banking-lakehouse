from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


def is_batch_successful(spark, batch_id, control_table):
    """
    Check whether a batch has already completed successfully.
    """

    return (
        spark.table(control_table)
        .filter(
            (F.col("batch_id") == batch_id) &
            (F.col("status") == "SUCCESS")
        )
        .limit(1)
        .count() > 0
    )


def record_batch_status(
    spark,
    batch_id,
    status,
    control_table,
    error_message=None
):
    """
    Record pipeline batch status as SUCCESS or FAILED.
    """

    if status not in ("SUCCESS", "FAILED"):
        raise ValueError(f"Invalid batch status: {status}")

    # Explicit schema prevents Spark inference errors
    status_schema = StructType([
        StructField("batch_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("error_messge", StringType(), True)
    ])

    status_record = (
        spark.createDataFrame(
            [(batch_id, status, error_message)],
            schema=status_schema
        )
        .withColumn(
            "processed_at",
            F.current_timestamp().cast("string")
        )
        .select(
            "batch_id",
            "status",
            "processed_at",
            "error_messge"
        )
    )

    (
        status_record.write
        .format("delta")
        .mode("append")
        .saveAsTable(control_table)
    )

    print(f"Batch {batch_id} marked {status}.")