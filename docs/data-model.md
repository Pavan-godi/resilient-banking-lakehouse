# Banking Data Model

## 1. Overview

The source database for this project is a banking database built in PostgreSQL.

It contains customer, account, branch, transaction and other banking-related information.

The transaction table is the primary source for the current ETL pipeline. Other entities provide the reference information needed to understand customer and account relationships.

## 2. Source Tables

The database currently contains the following tables:

| Table        | Purpose                                 |
| ------------ | --------------------------------------- |
| customers    | Stores customer information             |
| branches     | Stores bank branch information          |
| accounts     | Stores customer account information     |
| transactions | Stores banking transaction records      |
| loans        | Stores customer loan information        |
| credit_cards | Stores customer credit card information |
| employees    | Stores bank employee information        |

## 3. Table Relationships

The main relationships in the source database are:

* Each account is associated with a customer.
* Each account is associated with a branch.
* Transactions are associated with accounts.
* Loans are associated with customers.
* Credit cards are associated with customers.
* Employees are associated with branches.

The simplified relationship is:

```text
                 CUSTOMERS
                /    |     \
               /     |      \
          ACCOUNTS  LOANS  CREDIT_CARDS
             |
             |
          BRANCHES
             |
          EMPLOYEES

          ACCOUNTS
             |
             |
        TRANSACTIONS
```

The diagram represents the main business relationships, not the complete physical database schema.

## 4. Transaction Data Structure

The transaction table is the main input for the current ETL pipeline.

The fields used in the processing logic include:

| Column                | Purpose                                                   |
| --------------------- | --------------------------------------------------------- |
| transaction_id        | Identifies a transaction                                  |
| account_id            | Identifies the associated account                         |
| customer_id           | Identifies the associated customer                        |
| transaction_type      | Indicates DEBIT or CREDIT                                 |
| amount                | Transaction amount                                        |
| transaction_timestamp | Time when the transaction occurred                        |
| merchant              | Merchant associated with the transaction                  |
| status                | Transaction status                                        |
| updated_at            | Timestamp used to identify the latest transaction version |

The transaction_id is used for duplicate identification and incremental processing.

The updated_at field is used when selecting the latest version of a transaction.

## 5. Data Flow into the Lakehouse

Transaction records are ingested from the source and stored in the Bronze Delta table.

The current pipeline uses the following tables:

| Layer      | Table                   |
| ---------- | ----------------------- |
| Bronze     | bronze_transactions     |
| Silver     | silver_transactions     |
| Quarantine | quarantine_transactions |
| Gold       | gold_customer_metrics   |

Additional tables support batch tracking and incremental Gold processing:

| Table                       | Purpose                                        |
| --------------------------- | ---------------------------------------------- |
| etl_batch_control           | Tracks overall pipeline batch status           |
| gold_batch_control          | Tracks Gold batch application status           |
| gold_customer_contributions | Stores customer-level contributions by batch   |
| etl_dq_summary              | Stores data quality and reconciliation results |

All these tables are maintained in the Databricks workspace catalog.

## 6. Customer-Level Analytics

The Gold layer groups transaction information at the customer level.

The current analytical metrics include:

* Transaction count.
* Total debit amount.
* Total credit amount.
* Average transaction amount.
* Customer activity level.
* High-value customer indicator.

These metrics provide a customer-level view of transaction activity.

## 7. Data Model Considerations

A few points are important when working with this data:

1. Transaction records are identified using transaction_id.
2. Customer-level metrics are generated using customer_id.
3. Account relationships provide the connection between customers and transactions.
4. Duplicate transaction versions are handled using updated_at.
5. Batch identifiers help track which pipeline execution processed the data.

## 8. Future Enhancements

The data model can be extended to support:

* Historical customer dimension tracking.
* Slowly Changing Dimensions.
* Change Data Capture.
* Customer 360 analytical datasets.
* Additional account-level and branch-level reporting.

These enhancements will be added as the project evolves.
