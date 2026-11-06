# Resilient Banking Transaction Lakehouse

A Data Engineering project built using PostgreSQL, Python, PySpark, Databricks and Delta Lake.

The project processes banking transaction data through Bronze, Silver and Gold layers. The main focus is on handling invalid records, duplicate transactions, incremental updates and pipeline failures.

The data used in this project is synthetic.

## 1. Project Overview

The project starts with banking data stored in PostgreSQL and processes it using a Medallion architecture in Databricks.

The pipeline has three main stages:

- **Bronze:** stores ingested transaction data.
- **Silver:** validates records, handles duplicates and separates invalid records.
- **Gold:** generates customer-level metrics for analytical use.

I have also implemented batch tracking and failure-recovery logic to handle situations where a pipeline fails during execution.

## 2. Technology Stack
Component	Technology
Source database	PostgreSQL
Programming	Python
Query language	SQL
Distributed processing	Apache Spark, PySpark
Data platform	Databricks
Storage format	Delta Lake, Parquet
Version control	Git, GitHub
## 3. Pipeline Architecture
       PostgreSQL
            |
            v
      Data Ingestion
            |
            v
      BRONZE LAYER
      Raw Transactions
            |
            v
       SILVER LAYER
       Data Validation
       Duplicate Handling
       Quarantine Records
            |
            v
        GOLD LAYER
       Customer Metrics
            |
            v
      Analytical Tables
## 4. Bronze Layer

The Bronze layer receives transaction data from the source and stores it in Delta format.

The ingestion process includes:

- Adding a batch ID to identify each pipeline execution.
- Recording the ingestion timestamp.
- Checking existing transaction IDs before appending records.
- Processing newly identified records.

The purpose of this layer is to maintain the ingested data for downstream processing.

## 5. Silver Layer

The Silver layer applies validation and cleaning rules before records are used for analytics.

The current validation rules include:

Field	Validation
customer_id	Must not be NULL
amount	Must be greater than or equal to zero
transaction_type	Must be DEBIT or CREDIT

Records that fail validation are written to a separate quarantine table with the reason for rejection.

Duplicate transactions are identified using transaction_id. When multiple versions exist, the record with the latest updated_at value is selected.

The Silver table is maintained using Delta MERGE.

## 6. Gold Layer

The Gold layer produces customer-level analytical metrics.

The current metrics include:

- Transaction count.
- Total debit amount.
- Total credit amount.
- Average transaction amount.
- Customer activity classification.
- High-value customer indicator.

The pipeline maintains customer contributions at the batch level and uses them to rebuild metrics for affected customers.

This approach was introduced to handle retry scenarios where the Gold update succeeds but the pipeline fails before recording the batch as completed.

## 7. Data Quality and Reconciliation

The pipeline performs a reconciliation between the Bronze records and the records processed by the Silver layer.

The reconciliation checks:

Bronze Count = Valid Records + Quarantined Records + Duplicate Records

The pipeline records the data quality results in a Delta table.

Gold processing is allowed to continue only when the reconciliation check passes.

## 8. Pipeline Reliability

The project includes the following reliability mechanisms:

- **Batch-level idempotency**

  The pipeline checks the batch control table before processing. A batch already marked SUCCESS is skipped when the same batch is submitted again.

- **Duplicate handling**

  Transaction IDs are checked during ingestion and duplicate versions are handled during Silver processing.

- **Failure handling**

  Pipeline failures are recorded in the batch control table, and the exception is raised for visibility.

- **Gold retry recovery**

  Customer contributions are stored separately using batch and customer identifiers. Gold metrics are rebuilt from these contributions, allowing the pipeline to recover from a tested partial-failure scenario without adding the same batch contributions twice.

## 9. Structured Streaming Practice

I have also completed a separate Structured Streaming exercise in Databricks.

The exercise uses Parquet files as the input source and Delta as the output.

The concepts tested include:

- Streaming DataFrames.
- Micro-batch processing.
- AvailableNow trigger.
- Append, Update and Complete output modes.
- Checkpoint locations.
- Restarting a query using the same checkpoint.
- Processing newly added input records.

This is currently a separate learning exercise and is not yet integrated into the main banking transaction pipeline.

## 10. Current Project Status

The initial banking pipeline and its core reliability tests have been implemented.

The following areas are still under development:

- Auto Loader.
- Watermarking and late-arriving data.
- Event time and processing time.
- Change Data Capture (CDC).
- Slowly Changing Dimensions (SCD).
- Databricks Workflows and job orchestration.
- AWS S3, Glue and Athena integration.
- Automated unit and integration testing.
- Monitoring and performance benchmarking.

These features will be added and tested as the project develops.

## 11. Data Privacy

All banking records used in this project are synthetic. No actual customer banking information or personally identifiable financial data is used.

## 12. Project Objective


The objective is to build and document a banking data pipeline while gaining practical experience with data ingestion, distributed processing, Delta Lake, data quality, incremental processing and pipeline recovery.