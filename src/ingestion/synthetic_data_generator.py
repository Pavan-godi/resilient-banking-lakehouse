# ============================================================
# BANKING LAKEHOUSE - SYNTHETIC DATA GENERATOR
# ============================================================

from pyspark.sql import functions as F


def generate_synthetic_transactions(
    spark,
    record_count=10000,
    transaction_id_start=2000000,
    customer_id_start=100000,
    account_id_start=500000
):
    """
    Generate scalable synthetic banking transactions using
    Spark-native DataFrame operations.

    Data quality distribution:
    - 97% valid transactions
    - 1% missing customer ID
    - 1% negative transaction amount
    - 1% invalid transaction type

    The quality scenarios are mutually exclusive.

    Returns:
        Spark DataFrame containing synthetic transactions.
    """

    if record_count <= 0:
        raise ValueError("record_count must be greater than zero.")

    print(f"Generating {record_count} synthetic transactions...")

    # --------------------------------------------------------
    # 1. GENERATE TRANSACTION IDS
    # --------------------------------------------------------

    transactions = spark.range(0, record_count)

    transactions = transactions.withColumnRenamed(
        "id",
        "record_id"
    )

    # --------------------------------------------------------
    # 2. GENERATE BANKING IDENTIFIERS
    # --------------------------------------------------------

    transactions = (
        transactions
        .withColumn(
            "transaction_id",
            F.col("record_id") + transaction_id_start
        )
        .withColumn(
            "account_id",
            F.lit(account_id_start) +
            F.pmod(F.col("record_id"), F.lit(15000))
        )
        .withColumn(
            "customer_id",
            F.lit(customer_id_start) +
            F.pmod(F.col("record_id"), F.lit(10000))
        )
    )

    # --------------------------------------------------------
    # 3. GENERATE TRANSACTION DATES
    # --------------------------------------------------------

    transactions = transactions.withColumn(
        "transaction_date",
        F.date_format(
            F.date_add(
                F.to_date(F.lit("2026-01-01")),
                F.pmod(F.col("record_id"), F.lit(365)).cast("int")
            ),
            "yyyy-MM-dd"
        )
    )

    transactions = transactions.withColumn(
        "updated_at",
        F.concat(
            F.col("transaction_date"),
            F.lit(" 12:00:00")
        )
    )

    # --------------------------------------------------------
    # 4. GENERATE TRANSACTION TYPES
    # --------------------------------------------------------

    transactions = transactions.withColumn(
        "transaction_type",
        F.when(
            F.pmod(F.col("record_id"), F.lit(2)) == 0,
            "DEBIT"
        ).otherwise("CREDIT")
    )

    # --------------------------------------------------------
    # 5. GENERATE TRANSACTION AMOUNTS
    # --------------------------------------------------------

    transactions = transactions.withColumn(
        "amount",
        (
            F.pmod(
                F.col("record_id") * 7919,
                F.lit(500000)
            ) / 100.0
        ) + 10.0
    )

    # --------------------------------------------------------
    # 6. INTRODUCE CONTROLLED DATA QUALITY ISSUES
    # --------------------------------------------------------

    quality_code = F.pmod(
        F.col("record_id"),
        F.lit(100)
    )

    transactions = (
        transactions
        .withColumn(
            "customer_id",
            F.when(
                quality_code == 97,
                F.lit(None).cast("long")
            ).otherwise(F.col("customer_id"))
        )
        .withColumn(
            "amount",
            F.when(
                quality_code == 98,
                -F.abs(F.col("amount"))
            ).otherwise(F.col("amount"))
        )
        .withColumn(
            "transaction_type",
            F.when(
                quality_code == 99,
                F.lit("INVALID")
            ).otherwise(F.col("transaction_type"))
        )
    )

    # --------------------------------------------------------
    # 7. SELECT FINAL TRANSACTION COLUMNS
    # --------------------------------------------------------

    transactions = transactions.select(
        "transaction_id",
        "account_id",
        "customer_id",
        "transaction_date",
        "transaction_type",
        F.col("amount").cast("double").alias("amount"),
        "updated_at"
    )

    print("Synthetic transaction generation completed.")

    return transactions