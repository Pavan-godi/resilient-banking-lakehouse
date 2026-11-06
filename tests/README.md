# Test Strategy and Coverage

## Overview

Testing focuses on validating the banking transaction pipeline across ingestion, Bronze, Silver, Gold, data quality, and recovery scenarios.

The current tests are primarily executed through Databricks notebooks.

## Test Coverage

### 1. Source Ingestion
- Validate generated transaction schema.
- Check record counts and transaction ID uniqueness.
- Verify expected data quality issue distribution.

### 2. Bronze Layer
- Verify records are written to the Bronze Delta table.
- Rerun ingestion with the same batch ID.
- Confirm that retrying does not insert duplicate source records.

### 3. Silver Layer
- Validate required fields and transaction types.
- Verify invalid records are quarantined.
- Confirm duplicate handling and valid record counts.

### 4. Gold Layer
- Verify transaction contribution records.
- Validate customer-level aggregate metrics.
- Confirm that the Gold batch control records successful application.

### 5. Data Quality
- Verify record-count reconciliation.
- Confirm that the DQ result is persisted.
- Test failure handling after the DQ result is written.

### 6. Structured Streaming
- Verify initial file ingestion.
- Restart the query using the same checkpoint.
- Confirm that newly arriving files are processed incrementally.

## Current Test Limitations

- Automated unit-test execution is not yet implemented.
- Concurrent execution of the same batch is not covered.
- Full end-to-end streaming integration with the main batch pipeline is not implemented.
- Production-scale performance and recovery testing remain future work.