import pytest

from pyspark.sql import SparkSession

from src.ingestion.synthetic_data_generator import (
    generate_synthetic_transactions
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("BankingLakehouseTests")
        .getOrCreate()
    )

    yield session
    session.stop()


def test_generator_returns_requested_record_count(spark):
    df = generate_synthetic_transactions(spark, 100)
    assert df.count() == 100


def test_generator_has_expected_columns(spark):
    df = generate_synthetic_transactions(spark, 100)

    expected_columns = {
        "transaction_id",
        "account_id",
        "customer_id",
        "transaction_date",
        "transaction_type",
        "amount",
        "updated_at"
    }

    assert expected_columns == set(df.columns)


def test_generator_transaction_ids_are_unique(spark):
    df = generate_synthetic_transactions(spark, 100)

    assert df.select("transaction_id").distinct().count() == 100


def test_generator_quality_scenarios(spark):
    df = generate_synthetic_transactions(spark, 100)

    assert df.filter("customer_id IS NULL").count() == 1
    assert df.filter("amount < 0").count() == 1
    assert df.filter("transaction_type = 'INVALID'").count() == 1


def test_generator_rejects_zero_records(spark):
    with pytest.raises(ValueError):
        generate_synthetic_transactions(spark, 0)
