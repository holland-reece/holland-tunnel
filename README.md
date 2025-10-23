# 🏥 HollandTunnel: Multilayer Data Warehouse for NPI Physician Specialization Data (BigQuery + Airflow + dbt + BI)
  
Author: Holland Reece Brown  
🔗 [GitHub Profile](https://github.com/holland-reece) | 🌐 [Website](https://holland-reece.github.io/)  

## Overview

This project demonstrates a **modern data engineering pipeline** built around **Google BigQuery** and **open-source tools** — from ingestion and transformation to ML modeling and BI visualization.

The goal is to show real-world, production-style patterns on a small, well-scoped dataset: **National Provider Identifier (NPI) registry data** for healthcare professionals in the US.

---

## 🎯 Objectives

- Demonstrate my experience with **BigQuery**, **dbt**, and **BI tools**.
- Build a layered data warehouse: **Bronze → Silver → Gold → BI**.
- Implement a **DBSCAN clustering model** to visualize **geographical distributions of doctor specializations** across the US.

---

## 🧱 Tech Stack

| Layer | Tool | Purpose |
|-------|------|----------|
| Ingestion | **Airflow** | Orchestrate Python + API workflows |
| Data Lake | **Google Cloud Storage (GCS)** | Landing zone for raw NPI data |
| Warehouse | **BigQuery** | Central analytical database |
| Transformations | **dbt-bigquery** | SQL-based transformation & testing |
| Data Quality | **Great Expectations** | Validation & profiling |
| Lineage | **OpenLineage + Marquez** | Track data lineage from Airflow to BigQuery |
| ML | **scikit-learn (DBSCAN)** | Geographic clustering of NPI data |
| BI | **Apache Superset** | Visualization, dashboards, BI |
| (Future) | **Looker Studio** | Plan to explore Looker for viz/BI in future |
| (Future) | **Ollama (local LLM)** | Generate narrative reports from BI metrics |

---

## 🧮 Architecture Overview
```java
     ┌────────────────────────────┐
     │        NPI API (REST)      │
     └────────────┬───────────────┘
                  │
                  ▼
      ┌─────────────────────────┐
      │ Bronze Layer (Raw JSON) │
      │   → GCS: npi/bronze/    │
      └─────────────────────────┘
                  │
                  ▼
      ┌─────────────────────────┐
      │ Silver Layer (Cleaned)  │
      │   → BigQuery staging    │
      └─────────────────────────┘
                  │
                  ▼
      ┌─────────────────────────┐
      │ Gold Layer (Analytic)   │
      │   → BigQuery marts      │
      └─────────────────────────┘
                  │
                  ▼
    ┌────────────────────────────┐
    │ BI & ML (Superset/Looker) │
    │ DBSCAN clusters, maps, KPIs│
    └────────────────────────────┘

```
