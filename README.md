# DLT_BankingProject
A mini-project on banking data flow using Delta Live Tables in the Medallion Architecture.

# ✅ End-to-End Banking Transaction Pipeline using Databricks Delta Live Tables (DLT)

## Overview

This project demonstrates the implementation of an end-to-end **Banking Transaction Data Pipeline** using **Databricks Delta Live Tables (DLT)** following the **Medallion Architecture (Bronze → Silver → Gold)**.

The objective of this project is to simulate how a bank processes transaction data in real time, applies data quality validations, handles incremental updates using **AUTO CDC with SCD Type 2**, and generates business-ready reporting datasets.

The project showcases several important DLT concepts, including:

* Streaming ingestion
* Data quality expectations
* Incremental processing
* Change Data Capture (CDC)
* Slowly Changing Dimension (SCD) Type 2
* Gold-level business aggregations
* End-to-end pipeline validation

---

## Architecture

<img width="5970" height="1166" alt="image" src="https://github.com/user-attachments/assets/321178a4-74bf-4f35-95a3-c1a6fb5436d9" />


```text
Source Delta Table
(bank_transactions_raw)
           │
           ▼
Bronze Layer
(transactions)
           │
           ▼
Silver Layer
(transactions_refined)
           │
           ▼
Silver Historical Layer
(transactions_enr)
AUTO CDC + SCD Type 2
           │
           ▼
Gold Layer
(txn_aggregations)
```

---

## Business Scenario

Banks process thousands of transactions every day across multiple branches. These transactions may undergo validation, correction, reconciliation, or status updates from upstream systems.

This project simulates such a scenario by:

* Ingesting raw transaction data.
* Validating data quality.
* Dropping invalid transactions.
* Tracking transaction corrections using SCD Type 2.
* Producing branch-level reporting metrics.

---

## Technologies Used

* Databricks
* Delta Live Tables (DLT)
* PySpark
* Delta Lake
* SQL
* Structured Streaming
* AUTO CDC
* Slowly Changing Dimension (SCD) Type 2
* GitHub

---

## Dataset Description

The source table used in this project is:

### `bank_transactions_raw`

It contains banking transaction records with the following fields:

| Column        | Description                   |
| ------------- | ----------------------------- |
| txn_id        | Unique transaction identifier |
| customer_id   | Customer identifier           |
| account_no    | Account number                |
| txn_timestamp | Transaction timestamp         |
| txn_type      | DEBIT or CREDIT               |
| txn_amount    | Transaction amount            |
| currency      | Transaction currency          |
| branch_code   | Branch identifier             |
| txn_status    | SUCCESS or FAILED             |
| load_dt       | Source load date              |

---

## Source Data

The project uses two sets of data:

### Initial Load

* 15 transaction records.
* All records satisfy the business rules.

### Incremental Load

An additional 10 records are inserted into the same source table.

The incremental dataset contains both valid and invalid transactions to demonstrate DLT expectations.

Invalid records include:

| Transaction ID | Validation Failure          |
| -------------- | --------------------------- |
| TXN018         | Negative transaction amount |
| TXN019         | Invalid transaction type    |
| TXN020         | Invalid transaction status  |
| TXN021         | Missing customer identifier |

---

<img width="1911" height="907" alt="image" src="https://github.com/user-attachments/assets/e4a59e13-41d2-491f-928d-5f4ba0f1395a" />


# Bronze Layer

## Table: `transactions`

### Purpose

The Bronze layer is responsible for ingesting raw transaction data from the source Delta table using Structured Streaming.

### Key Features

* Streaming ingestion.
* Preserves source data.
* Adds ingestion metadata.

### Additional Metadata

An ingestion timestamp is generated to track when records entered the pipeline.

Example:

```text
ingestion_ts
```

---

# Silver Layer

## View: `transactions_refined`

### Purpose

The Silver layer applies data quality validations and filters out invalid transactions before downstream consumption.

---

## DLT Expectations

### Monitoring Expectations

These expectations monitor data quality without dropping records.

| Rule               | Description                       |
| ------------------ | --------------------------------- |
| txn_id IS NOT NULL | Transaction identifier must exist |
| currency = 'INR'   | Only INR transactions are allowed |

Implemented using:

```python
@dlt.expect_all(...)
```

---

## Cleansing Expectations

These expectations remove invalid records.

| Rule                               | Description                  |
| ---------------------------------- | ---------------------------- |
| txn_amount > 0                     | Amount must be positive      |
| txn_type IN ('DEBIT', 'CREDIT')     | Valid transaction types only |
| txn_status IN ('SUCCESS',' FAILED') | Valid statuses only          |
| customer_id IS NOT NULL            | Customer must exist          |

Implemented using:

```python
@dlt.expect_all_or_drop(...)
```

---

## Silver Validation Results

### Initial Load

<img width="1523" height="712" alt="image" src="https://github.com/user-attachments/assets/07a2317c-562b-4f5b-a7b3-9336caeaf84f" />


Source Records:

```text
15
```

Records Dropped:

```text
0
```

Silver Output:

```text
15
```

---

### Incremental Load

<img width="937" height="278" alt="image" src="https://github.com/user-attachments/assets/180b4d49-3b56-49db-bb88-4f5b5e47f636" />


Additional Source Records:

```text
10
```

Invalid Records Dropped:

```text
4
```

Silver Output After Incremental Processing:

```text
21
```

---

# Historical Layer (AUTO CDC)

## Table: `transactions_enr`

### Purpose

This layer maintains the historical versions of transactions using Delta Live Tables AUTO CDC.

The implementation demonstrates how corrected transactions can be tracked over time.

---

## CDC Configuration

### Key

```text
txn_id
```

### Sequence Column

```text
txn_timestamp
```

### Storage Type

```text
SCD Type 2
```

---

## SCD Type 2 Behaviour

DLT automatically generates historical tracking columns.

Examples include:

```text
__START_AT
__END_AT
```

---

## Example Scenario

Transaction:

```text
TXN010
```

appears in both the initial and incremental datasets.

The incremental occurrence is treated as a newer version.

DLT automatically:

* Closes the old version.
* Creates a new active version.

This preserves historical lineage.

---

## Historical Validation Results

### Initial Load

Historical Records:

```text
15
```

---

### After Incremental Load

<img width="1819" height="752" alt="image" src="https://github.com/user-attachments/assets/8f84729a-de5b-4bdd-96c7-5e75319adcbc" />


Valid Incremental Records:

```text
6
```

Historical Versions:

```text
21
```

---

# Gold Layer

## Table: `txn_aggregations`

### Purpose

The Gold layer provides business-ready reporting datasets.

Branch-level KPIs are generated from the active versions of transactions.

Only current records are considered:

```text
__END_AT IS NULL
```

---

## Metrics Generated

| Metric                  | Description                       |
| ----------------------- | --------------------------------- |
| total_transactions      | Total number of transactions      |
| successful_transactions | Number of successful transactions |
| failed_transactions     | Number of failed transactions     |
| total_debit_amount      | Total debit amount                |
| total_credit_amount     | Total credit amount               |

---

## Gold Validation Results

The final Gold dataset contains:

```text
6 records
```

representing:

```text
3 Branches × 2 Business Dates
```

---

## Sample Gold Output

| Transaction Date | Branch | Total Transactions |
| ---------------- | ------ | ------------------ |
| 2026-06-01       | BR001  | 6                  |
| 2026-06-01       | BR002  | 4                  |
| 2026-06-01       | BR003  | 4                  |
| 2026-06-02       | BR001  | 2                  |
| 2026-06-02       | BR002  | 3                  |
| 2026-06-02       | BR003  | 1                  |

---

# Testing and Validation

This project validates the pipeline using both initial and incremental loads.

The following scenarios were tested successfully:

* Initial pipeline execution.
* Incremental processing.
* Data quality validations.
* Invalid record rejection.
* CDC processing.
* SCD Type 2 history generation.
* Historical version verification.
* Gold-level aggregation validation.

---

# Repository Structure

```text
banking-dlt-pipeline/
│
├── bronze/
│   └── ingestion.py
│
├── silver/
│   └── transformations.py
│
├── gold/
│   └── aggregations.py
│
├── source_data/
│   ├── create_tables.sql
│   ├── initial_load.sql
│   └── incremental_load.sql
│
├── screenshots/
│   ├── pipeline_dag.png
│   ├── expectations.png
│   ├── scd_validation.png
│   └── gold_output.png
│
└── README.md
```

---

# Key Learnings

This project demonstrates practical implementation of several important Databricks concepts:

* Designing Medallion Architectures.
* Building streaming pipelines using DLT.
* Applying data quality checks using Expectations.
* Handling incremental data loads.
* Implementing AUTO CDC.
* Managing historical records using SCD Type 2.
* Generating business-facing Gold datasets.
* Validating pipeline outputs through end-to-end testing.

---

# Conclusion

This project simulates a realistic banking transaction processing pipeline using Databricks Delta Live Tables.

It highlights how modern data engineering practices can be used to transform raw streaming data into trusted, business-ready datasets while maintaining data quality and historical traceability.

The implementation emphasizes not only pipeline development but also testing, validation, and reasoning about business outcomes, which are critical aspects of production-grade data engineering systems.

