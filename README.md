# GA4 Marketing Reporting Pipeline

## 1. Background

This repository contains SQL-based marketing analytics models built on top of **Google Analytics 4 (GA4) BigQuery export data**.  
The primary goal is to transform raw GA4 event data into clean, analytics‑ready reporting tables for:

- **Core Web & Performance**
- **Page-Level Performance**
- **Session Acquisition**
- **User Journey & Cohorts**

Each report rebuilds a **daily partition** using an idempotent pattern ("Rebuild the slice"), ensuring consistency and preventing duplication regardless of the DAG run frequency.

---

## 2. Architecture Overview

### **GA4 → BigQuery Native Export**
GA4 automatically exports its events into partitioned tables in BigQuery using the format:

```
events_YYYYMMDD
```

These are stored under:

```
marketing_data.analytics_123456789.events_*
```

### **Cloud Composer (Managed Airflow)**
Cloud Composer provides:

- Orchestration of SQL transformations  
- Scheduled rebuilding of report partitions  
- Automatically passing `{{ ds_nodash }}` for daily slice processing  
- Retry & monitoring capabilities  

### **Transformation Pattern (Solution A – Rebuild Slice)**

Each DAG run:

1. Looks at the current partition date (`{{ ds_nodash }}`)
2. Filters GA4 events for that date  
3. Rebuilds the corresponding report table’s partition using:

```sql
CREATE OR REPLACE TABLE dataset.table$20250101 AS (...)
```

This ensures:

- No duplicate historical data
- Consistent rebuilding if the logic changes
- Safe to re-run multiple times per day

---

## 3. Report Tables

| Report | Output Table | Purpose |
|--------|--------------|---------|
| Core Web Performance | `core_web_performance` | Traffic, devices, engagement & revenue KPIs |
| Page-Level Performance | `page_level_performance` | Page-level metrics & ranking insights |
| Session Acquisition | `session_acquisition` | Traffic source performance & user acquisition |
| User Journey & Cohorts | `user_journey_cohorts` | Cohort behavior, lifetime events & retention |

SQL files are stored in the `sql_queries/` directory and dynamically loaded by the DAG.

---

## 4. Cloud Composer Deployment Steps

### **Step 1 – Create Composer Environment**
In GCP Console:

```
Composer → Create Environment
Python: 3.10+
Environment type: Composer 2+
```

### **Step 2 – Upload DAG**
Upload:

```
dags/
  ga4_marketing_reports_dag.py
sql_queries/
  core_web_performance.sql
  page_level_performance.sql
  session_acquisition.sql
  user_journey_analysis.sql
```

### **Step 3 – DAG Scheduling**
The DAG runs **hourly**, but safely overwrites only the date partition.

---

## 5. Example Architecture Diagram (ASCII)

```
                +-----------------------+
                |     Google Analytics  |
                |         (GA4)         |
                +-----------+-----------+
                            |
                            | Native Export (Daily)
                            v
                +----------------------------+
                |      BigQuery Raw GA4      |
                |  analytics_123.events_*    |
                +-------------+--------------+
                              |
                              | SQL Transformations (Hourly)
                              v
                +-----------------------------+
                |  Cloud Composer (Airflow)   |
                |  DAG: rebuild daily slices  |
                +-------------+---------------+
                              |
                              v
           +----------------------------------------------+
           |           BigQuery Reporting Tables          |
           |  marketing_reports.core_web_performance      |
           |  marketing_reports.page_level_performance    |
           |  marketing_reports.session_acquisition       |
           |  marketing_reports.user_journey_cohorts      |
           +----------------------------------------------+
```

---

## 6. Engineer Onboarding Guide

### **Prerequisites**
- Access to BigQuery datasets:
  - `marketing_data.analytics_123456789`
  - `marketing_reports`
- IAM Roles:
  - BigQuery Admin or Data Editor
  - Composer Worker
  - Storage Object Admin (for DAG uploads)
- Python & basic SQL familiarity

---

### **Local Setup**
Clone the repo:

```bash
git clone <your-repo-url>
cd ga4-marketing-pipeline
```

(Optional) Install Airflow locally:

```bash
pip install "apache-airflow==2.9.0"
```

---

### **Testing SQL**
Run any SQL file locally:

```bash
bq query --use_legacy_sql=false < sql_queries/core_web_performance.sql
```

---

### **Deploying Updates**
1. Edit SQL or DAG
2. Upload to Composer bucket:
   ```
   gsutil cp dags/ga4_marketing_reports_dag.py gs://<composer-bucket>/dags/
   gsutil cp sql_queries/*.sql gs://<composer-bucket>/dags/sql_queries/
   ```
3. Composer redeploys automatically
4. Verify DAG in Airflow UI

---

## 7. Git Workflow Tips

Undo the last commit but keep changes:

```bash
git reset --soft HEAD~1
```

Undo last commit and discard changes:

```bash
git reset --hard HEAD~1
```

---

## 8. Notes & Best Practices

### **Partitioning**
Ensure all reporting tables are **partitioned by event_date**.

### **Backfills**
Enable on-demand backfills with:

```
airflow dags backfill ga4_marketing_reports_rebuild_slice_hourly -s 2025-01-01 -e 2025-01-10
```

### **Monitoring**
Use Composer’s Airflow UI:

- DAG run history  
- Task-level logs  
- Retry failures  

---

## 9. Summary

This pipeline provides:

- Automated hourly analytics model updates  
- Clean partition-based tables ready for dashboards  
- Scalable and cost-efficient processing  
- Separation of logic (SQL) and orchestration (Airflow)  

It is production-grade, idempotent, and suitable for long-term marketing analytics growth.
