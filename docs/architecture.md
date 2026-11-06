# Banking Lakehouse Architecture

## 1. Overview

This project uses a Medallion architecture to process banking transaction data.

PostgreSQL is used as the source database, and Databricks with Delta Lake is used for data processing and storage.

The pipeline separates raw ingestion, data validation and analytical transformations into three layers: Bronze, Silver and Gold.

The main reason for separating these stages is to make the processing easier to understand, maintain and troubleshoot.

## 2. High-Level Data Flow

```text
       PostgreSQL
            |
            v
      Source Ingestion
            |
            v
      Bronze Delta Table
            |
            v
      Silver Processing
            |
       +----+----+
       |         |
       v         v
   Valid Data  Invalid Data
       |         |
       v         v
   Silver Table  Quarantine Table
       |
       v
   Gold Processing
       |
       v
   Customer Metrics
```

## 3. Source Data

The project uses a banking database containing entities such as:

* Customers
* Accounts
* Branches
* Transactions
* Loans
* Credit Cards
* Employees

The transaction data is the main input for the current processing pipeline.

The records contain fields such as transaction_id, customer_id, account_id, transaction_type, amount, transaction_timestamp and updated_at.

All records are synthetic and are used only for development and testing.

## 4. Bronze Layer

The Bronze layer stores the ingested transaction data in Delta format.

During ingestion, the pipeline adds:

* batch_id to identify the processing batch.
* ingestion timestamp to record when the data entered the pipeline.

Before appending records, the pipeline checks existing transaction IDs to avoid inserting records that have already been ingested.

The current batch records are then made available for Silver processing.

## 5. Silver Layer

The Silver layer is responsible for validating and cleaning transaction records.

The following checks are currently implemented:

1. customer_id must not be NULL.
2. Transaction amount must be greater than or equal to zero.
3. transaction_type must be either DEBIT or CREDIT.

Records that fail validation are written to the quarantine table along with their rejection reason.

Duplicate transactions are identified using transaction_id. When multiple versions of a transaction exist, the latest record based on updated_at is selected.

The cleaned records are written to the Silver Delta table using MERGE.

This allows existing transaction records to be updated when a newer version is available.

## 6. Gold Layer

The Gold layer contains customer-level analytical metrics.

The pipeline calculates:

* Total transaction count.
* Total debit amount.
* Total credit amount.
* Average transaction amount.
* Customer activity level.
* High-value customer indicator.

Instead of directly adding every batch result to the existing Gold metrics, the pipeline maintains a separate contribution table.

The contribution table uses batch_id and customer_id to identify the contribution made by each customer in a particular batch.

Gold metrics are rebuilt for affected customers using these stored contributions.

This design helps prevent double counting during retry scenarios.

## 7. Data Quality and Reconciliation

The pipeline maintains a reconciliation check between Bronze and Silver processing.

The expected relationship is:

Bronze Count = Valid Count + Quarantine Count + Duplicate Count

The duplicate count represents additional duplicate records identified during processing.

The reconciliation result is stored in a separate Delta table.

Gold processing is allowed to continue only when the reconciliation check passes.

## 8. Pipeline Control and Failure Recovery

Two control tables are used:

* etl_batch_control
* gold_batch_control

The first table tracks the overall pipeline batch status.

The second table tracks whether Gold processing has been applied for a particular batch.

The pipeline also uses a failure handler to record failed execution status and raise the exception.

During development, I deliberately introduced a failure after the Gold metrics MERGE but before the Gold batch control update.

This helped test whether a retry would add the same customer contributions again.

The contribution-based approach allowed the pipeline to rebuild the affected Gold metrics without duplicating the batch contributions.

## 9. Structured Streaming

A separate Structured Streaming exercise was completed in Databricks.

It reads Parquet files as a streaming source and writes the processed records to Delta using a checkpoint location.

The exercise covers incremental file processing, micro-batches, AvailableNow triggers and query recovery.

This streaming exercise is not yet connected to the main banking pipeline.

## 10. Current Architecture Scope

The current implementation focuses on transaction ingestion, validation, deduplication, reconciliation, customer analytics and failure recovery.

The following architecture enhancements are planned:

* AWS S3 integration.
* Auto Loader for file ingestion.
* CDC processing.
* SCD Type 1 and Type 2.
* Late-arriving data handling.
* Databricks Workflows.
* Monitoring and automated testing.

These will be added after implementation and testing.

## 11. Design Considerations

A few decisions guide the current implementation:

* Delta tables are used to support transactional data operations.
* Bronze, Silver and Gold are separated to keep ingestion, cleaning and analytics independent.
* Invalid records are stored separately instead of being silently discarded.
* MERGE is used for incremental updates.
* Batch identifiers help track individual pipeline executions.
* Customer contributions provide a recoverable basis for rebuilding Gold metrics.
* Reconciliation prevents downstream processing when the expected record counts do not match.
