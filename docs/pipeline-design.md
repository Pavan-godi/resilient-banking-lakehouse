# Banking Transaction Pipeline Design

## 1. Purpose

This document explains how the banking transaction pipeline is organised and how data moves through its different processing stages.

The pipeline is implemented using PySpark and Delta Lake in Databricks.

The main objective is to process transaction records in batches while maintaining data quality and handling repeated executions safely.

## 2. Pipeline Execution Flow

The current pipeline follows this sequence:

```text
        Start Pipeline
              |
              v
       Batch ID Check
              |
       +------+------+
       |             |
   Already       New Batch
   SUCCESS          |
       |             v
      Skip       Bronze Ingestion
                     |
                     v
               Silver Validation
                     |
              +------+------+
              |             |
          Valid Data    Invalid Data
              |             |
              v             v
         Deduplication   Quarantine
              |
              v
          Silver MERGE
              |
              v
        DQ Reconciliation
              |
              v
        Gold Batch Check
              |
              v
       Customer Contributions
              |
              v
       Rebuild Gold Metrics
              |
              v
       Update Gold Control
              |
              v
       Mark Pipeline SUCCESS
```

## 3. Batch Identification

Each pipeline execution is associated with a batch_id.

Before processing begins, the pipeline checks the overall batch control table.

If the same batch_id is already marked SUCCESS, the pipeline skips processing.

This prevents a successfully completed batch from being processed again unnecessarily.

## 4. Bronze Processing

The ingestion stage adds batch information and an ingestion timestamp to the incoming transaction records.

Before inserting records into Bronze, the pipeline checks which transaction IDs already exist.

A left anti join is used to identify records that are not already present.

Only the new records are appended to the Bronze Delta table.

The current batch records are then selected for downstream processing.

## 5. Silver Processing

The Silver stage performs three main operations.

### 5.1 Validation

The pipeline checks:

* customer_id is not NULL.
* amount is greater than or equal to zero.
* transaction_type is DEBIT or CREDIT.

Invalid records are separated from valid records.

### 5.2 Quarantine

Invalid records are written to the quarantine Delta table along with the rejection reason.

A Delta MERGE is used so that retrying the same batch does not repeatedly insert the same quarantined records.

### 5.3 Deduplication

The pipeline identifies duplicate transaction IDs.

When multiple versions of a transaction exist, a window function with ROW_NUMBER is used to select the latest record based on updated_at.

The deduplicated records are then merged into the Silver Delta table.

The MERGE updates an existing transaction when the incoming version is newer and inserts records that do not already exist.

## 6. Data Quality Reconciliation

After Silver processing, the pipeline compares the Bronze record count with the valid, quarantined and duplicate counts.

The expected relationship is:

Bronze Count = Valid Count + Quarantine Count + Duplicate Count

The result is recorded in the etl_dq_summary table.

If reconciliation fails, Gold processing is stopped.

This prevents downstream analytics from being updated using a batch that has not passed the expected record-count check.

## 7. Gold Processing

Gold processing maintains customer-level transaction metrics.

Before applying a batch, the pipeline checks gold_batch_control to determine whether that batch has already been applied.

For a new batch:

1. Customer-level transaction metrics are calculated.
2. The results are stored in gold_customer_contributions.
3. The contribution records are identified using batch_id and customer_id.
4. Affected customers are identified.
5. Their Gold metrics are rebuilt using the stored contributions.
6. The resulting records are merged into gold_customer_metrics.

The contribution table provides a durable record of the batch-level input used to calculate Gold metrics.

## 8. Failure Handling

The pipeline contains a failure handler.

If an exception occurs during execution:

* The batch status is updated to FAILED.
* The exception is raised again.
* The failure remains visible to the caller.

A retry can then be initiated after addressing the failure.

The Gold contribution design was tested using an intentional failure after the Gold metrics MERGE but before the Gold batch control update.

On retry, the existing contributions were reused to rebuild the affected customer metrics.

This test demonstrated that the same batch contributions were not added twice.

## 9. Control Tables

The pipeline uses the following control and tracking tables:

| Table                       | Responsibility                             |
| --------------------------- | ------------------------------------------ |
| etl_batch_control           | Tracks overall batch execution status      |
| gold_batch_control          | Tracks whether Gold processing was applied |
| etl_dq_summary              | Stores reconciliation results              |
| gold_customer_contributions | Stores batch-level customer contributions  |

These tables help track execution and support recovery during partial failures.

## 10. Important Implementation Considerations

A few distinctions are important:

* Batch-level idempotency and transaction-level deduplication solve different problems.
* Checkpointing in Structured Streaming is separate from the batch control mechanism used by this pipeline.
* Delta MERGE handles incremental inserts and updates, but duplicate source keys must be resolved before the MERGE.
* A successful Silver operation does not automatically mean Gold processing has completed.
* The Gold contribution table helps recover from a tested partial-failure scenario.

## 11. Current Limitations and Planned Work

The current pipeline is batch-oriented.

The following enhancements are still planned:

* Integration of the streaming exercise into the main pipeline.
* Auto Loader for incremental file ingestion.
* CDC processing.
* Late-arriving transaction handling.
* SCD Type 1 and Type 2.
* Databricks Workflows for orchestration.
* Automated integration and regression testing.
* AWS-based ingestion and storage.
