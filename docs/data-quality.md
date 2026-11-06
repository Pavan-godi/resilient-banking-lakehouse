# Data Quality and Validation

## 1. Overview

Data quality checks are performed in the Silver layer before transaction records are used for customer-level analytics.

The purpose is to identify invalid records, handle duplicate transactions and verify that the records processed by the pipeline are accounted for.

## 2. Validation Rules

The current pipeline implements three primary validation rules.

| Field            | Rule                    | Reason                                                                      |
| ---------------- | ----------------------- | --------------------------------------------------------------------------- |
| customer_id      | Must not be NULL        | Every transaction must be associated with a customer                        |
| amount           | Must be >= 0            | Negative transaction amounts are not accepted by the current pipeline rules |
| transaction_type | Must be DEBIT or CREDIT | Only supported transaction types are accepted                               |

Records that fail these checks are classified as invalid.

## 3. Quarantine Processing

Invalid records are written to a separate Delta table:

`quarantine_transactions`

The pipeline also records the reason why each record failed validation.

This keeps rejected records available for investigation instead of silently removing them.

The quarantine write uses Delta MERGE to support retry-safe processing.

## 4. Duplicate Detection

The pipeline identifies duplicate transaction records using `transaction_id`.

When multiple versions of the same transaction are present, the latest record is selected using the `updated_at` timestamp.

A ROW_NUMBER window function is used to rank the available versions.

The record with the latest timestamp is retained for Silver processing.

The duplicate count represents the number of additional records removed during deduplication.

## 5. Data Quality Reconciliation

After validation and deduplication, the pipeline checks whether the Bronze records are accounted for.

The expected relationship is:

```text
Bronze Count
    =
Valid Count
    +
Quarantine Count
    +
Duplicate Count
```

The reconciliation result is stored in:

`etl_dq_summary`

Gold processing is allowed to continue only when this check passes.

## 6. Data Quality Test Scenarios

The current validation logic covers the following scenarios:

| Scenario                     | Expected result                    |
| ---------------------------- | ---------------------------------- |
| Missing customer_id          | Quarantine                         |
| Negative amount              | Quarantine                         |
| Unsupported transaction type | Quarantine                         |
| Duplicate transaction_id     | Deduplicate                        |
| Valid transaction            | Silver processing                  |
| Repeated quarantine batch    | Avoid duplicate quarantine inserts |

## 7. Current Implementation

The data quality logic is part of the main banking transaction pipeline.

The current implementation focuses on record-level validation, duplicate handling and count reconciliation.

Further enhancements can include:

* Referential integrity checks against customer and account tables.
* Mandatory-field completeness checks.
* Business-specific amount thresholds.
* Data freshness checks.
* Automated data quality test suites.
* Data quality trend monitoring.

These enhancements will be implemented and tested separately.
