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
