# SQL Server CDC to BigQuery using Dataflow

This repository contains a custom pipeline to stream **Change Data Capture (CDC)** data from SQL Server to **Google BigQuery**, using **Apache Beam (Dataflow)**, without Kafka or Debezium.

---

## 📌 Overview

This pipeline reads changes from SQL Server's log-based CDC tables (e.g., `cdc.fn_cdc_get_all_changes_<table>`) and streams them directly to BigQuery. It is ideal for teams who want a simplified CDC architecture with:

- **No Kafka**, **No Pub/Sub**, and **No Debezium**
- End-to-end data flow using only **SQL Server + Dataflow + BigQuery**
- Low-latency change propagation and simplified operations

---

## 📊 Architecture
<img width="909" alt="Screenshot 2025-05-14 at 20 30 05" src="https://github.com/user-attachments/assets/57dddde3-91d3-43f4-a81b-afcf598861e5" />

## 🛠️ Setup Instructions

### 1. Create Table for CDC in SQL Server

```sql
CREATE TABLE dbo.produk (
    idproduk INT PRIMARY KEY,
    namaproduk VARCHAR(255),
    hargaproduk DECIMAL(18,2),
    tanggalupdate DATETIME
);

INSERT INTO dbo.produk (idproduk, namaproduk, hargaproduk, tanggalupdate)
VALUES 
    (3, 'Susu UHT', 5000.00, GETDATE()),
    (4, 'Gula Pasir', 7500.00, GETDATE());

-- Example update
UPDATE dbo.produk
SET hargaproduk = 8000.00,
    tanggalupdate = GETDATE()
WHERE idproduk = 4;
```

### 2. Enable CDC on the Database and Table

```sql
-- Enable CDC on the database
EXEC sys.sp_cdc_enable_db;

-- Enable CDC on the "produk" table
EXEC sys.sp_cdc_enable_table
  @source_schema = 'dbo',
  @source_name = 'produk',
  @role_name = NULL;
```

### 3. Create Staging Table and Stored Procedure to Extract CDC

```sql
-- Create staging table for CDC data
CREATE TABLE dbo.staging_produk_cdc (
    lsn VARCHAR(50),
    seqval VARCHAR(50),
    operasi VARCHAR(50),
    idproduk INT,
    namaproduk VARCHAR(255),
    hargaproduk DECIMAL(18,2),
    tanggalupdate DATETIME
);
```

```sql
-- Stored Procedure to capture changes
CREATE PROCEDURE dbo.usp_capture_produk_cdc
AS
BEGIN
    DECLARE @from_lsn BINARY(10), @to_lsn BINARY(10);

    SET @from_lsn = sys.fn_cdc_get_min_lsn('dbo_produk');
    SET @to_lsn   = sys.fn_cdc_get_max_lsn();

    -- Clear existing staging data
    DELETE FROM dbo.staging_produk_cdc;

    -- Insert CDC changes into staging
    INSERT INTO dbo.staging_produk_cdc (
        lsn, seqval, operasi, idproduk, namaproduk, hargaproduk, tanggalupdate
    )
    SELECT 
        sys.fn_varbintohexstr(__$start_lsn),
        sys.fn_varbintohexstr(__$seqval),
        CASE __$operation
            WHEN 1 THEN 'DELETE'
            WHEN 2 THEN 'INSERT'
            WHEN 3 THEN 'UPDATE - BEFORE'
            WHEN 4 THEN 'UPDATE - AFTER'
        END,
        idproduk,
        namaproduk,
        hargaproduk,
        tanggalupdate
    FROM cdc.fn_cdc_get_all_changes_dbo_produk(@from_lsn, @to_lsn, 'all update old')
    ORDER BY __$start_lsn, __$seqval;
END;
```

### 4. Create Table in BigQuery (Staging)

```sql
CREATE OR REPLACE TABLE `your_project.your_dataset.staging_produk_cdc` (
  lsn STRING,
  seqval STRING,
  operasi STRING,
  idproduk INT64,
  namaproduk STRING,
  hargaproduk NUMERIC,
  tanggalupdate DATETIME
);
```

---

## 🚀 Running the Pipeline

You can run the pipeline with Apache Beam or package it as a Flex Template:

```bash
python main.py \
  --runner DataflowRunner \
  --project [GCP_PROJECT_ID] \
  --region [REGION] \
  --temp_location gs://[BUCKET]/temp \
  --staging_location gs://[BUCKET]/staging \
  --input_table "dbo.staging_produk_cdc" \
  --output_table "your_project.your_dataset.staging_produk_cdc"
```

---

## ✅ Features

- Log-based CDC (low impact on OLTP)
- Idempotent ingestion using LSN + SEQVAL
- Native BigQuery support
- Optional LSN checkpointing for resume
- Fully-managed autoscaling via Google Cloud Dataflow

---

## ⚠️ Limitations

- Schema evolution not supported (manual update required)
- No buffering layer (Pub/Sub); if pipeline fails, data can be lost
- JDBC access must be secured via VPN/PSC

---

## 📈 Monitoring & Tips

- Monitor LSN lag vs. latest ingested row
- Store last LSN in Cloud Storage or BigQuery
- Add deduplication logic in final table using MERGE

---

## 📄 License

MIT License.

---
