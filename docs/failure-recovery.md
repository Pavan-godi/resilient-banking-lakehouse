# Pipeline Failure Handling and Recovery

## 1. Why Failure Recovery Matters

A data pipeline does not always complete successfully.

For example, a pipeline might update an analytical table and then fail before recording the batch as completed.

If the same batch is executed again without considering what happened during the previous attempt, the pipeline could process the same data twice.

This document explains how failure handling and retry recovery are implemented in the banking transaction pipeline.

## 2. Batch Execution Tracking

The pipeline uses two control tables:

| Table              | Purpose                                         |
| ------------------ | ----------------------------------------------- |
| etl_batch_control  | Tracks the overall pipeline execution status    |
| gold_batch_control | Tracks whether Gold processing has been applied |

The overall batch status can be used to distinguish successful and failed executions.

A batch already marked SUCCESS is skipped when the same batch_id is submitted again.

## 3. Failure Scenario Tested

During development, I deliberately introduced a failure at a specific point in Gold processing.

The failure was triggered:

* After the Gold customer metrics MERGE completed.
* Before the Gold batch control table was updated.

This created a partial-failure scenario.

The Gold table had already been modified, but the pipeline had not recorded the batch as successfully applied.

This is an important situation to test because a retry must not add the same customer contributions again.

## 4. Problem with a Simple Retry

Consider the following execution:

```text
Batch starts
    |
    v
Calculate customer metrics
    |
    v
Update Gold table
    |
    v
Pipeline fails
    |
    v
Gold control not updated
    |
    v
Retry begins
```

If the retry simply adds the same metrics to the existing Gold values, customer transaction counts and amounts could be double-counted.

Updating the batch control table alone does not solve this problem because the failure occurred before that update.

## 5. Recovery Design

To address this scenario, the pipeline uses a separate contribution table.

Table:

gold_customer_contributions

The contribution records are identified using:

* batch_id
* customer_id

The pipeline uses Delta MERGE to maintain these contributions.

During Gold processing:

1. Customer-level metrics are calculated for the current batch.
2. The contributions are merged into the contribution table.
3. Affected customers are identified.
4. Their Gold metrics are rebuilt using the stored contributions.
5. The final customer metrics are merged into the Gold table.
6. The Gold batch control is updated.

This allows the pipeline to rebuild the affected customer metrics from the stored batch contributions instead of adding the same batch results to the previous Gold values.

## 6. Retry Execution

The recovery sequence is:

```text
       Retry Begins
            |
            v
   Check Gold Batch Status
            |
            v
   Merge Customer Contributions
            |
            v
   Identify Affected Customers
            |
            v
   Rebuild Customer Metrics
            |
            v
   Merge Gold Metrics
            |
            v
   Update Gold Batch Control
            |
            v
     Continue Pipeline
            |
            v
     Mark Batch SUCCESS
```

The contribution MERGE and Gold rebuild are designed to make the tested retry scenario safe.

## 7. Test Performed

The following test was performed during development:

**Test:** Partial failure after Gold metrics MERGE.

**Execution:**

1. Started a new pipeline batch.
2. Allowed Gold customer metrics to be updated.
3. Triggered an intentional failure before updating Gold batch control.
4. Confirmed that the overall pipeline recorded the failure.
5. Removed the failure injection.
6. Retried the same batch.
7. Checked the Gold customer metrics and contribution records.
8. Verified that the retry did not add the same batch contributions again.
9. Confirmed that Gold batch control was updated and the pipeline completed successfully.

**Result:** PASS for the tested partial-failure scenario.

## 8. Important Distinctions

A few mechanisms in this project solve different problems:

| Mechanism                 | Responsibility                                 |
| ------------------------- | ---------------------------------------------- |
| Batch control             | Tracks pipeline execution status               |
| Transaction deduplication | Identifies repeated business transactions      |
| Delta MERGE               | Applies inserts and updates                    |
| Contribution table        | Maintains batch-level customer contributions   |
| Checkpointing             | Stores Structured Streaming progress and state |

These mechanisms should not be treated as interchangeable.

For example, a streaming checkpoint does not automatically prevent duplicate business transactions.

## 9. Current Limitations

The failure-recovery test covers a specific partial-failure scenario in Gold processing.

It does not establish that every possible failure point has been tested.

Further testing is required for:

* Failure during Bronze ingestion.
* Failure during Silver processing.
* Failure during quarantine writes.
* Failure during data quality reconciliation.
* Concurrent execution of the same batch.
* Recovery after source data changes.
* Recovery after a checkpoint is lost or replaced.

These scenarios will be addressed as the pipeline is hardened.
