# 🛒 E-Commerce Batch Processing Pipeline
### Data Engineering Portfolio — DLMDSEDE02 | IU International University

---

## 📌 What This Project Does

This project builds a **fully automated batch-processing data pipeline** for an e-commerce machine learning application.

It takes raw e-commerce transaction data from **1 million+ records**, ingests it through Apache Kafka, stores it in PostgreSQL, and every quarter Apache Airflow automatically triggers an aggregation job that produces a clean feature table — ready for a machine learning model to train on.

Everything runs as **isolated Docker microservices** using Docker Compose — meaning anyone can download this repository and run the entire pipeline on their own machine with just one command.

---

## 🏗️ Architecture

```
Olist CSV Files (Kaggle)
        │
        ▼
  Kafka Producer
  (reads CSVs, sends JSON messages)
        │
        ▼
  Kafka Broker
  (topic: ecom_transactions)
        │
        ▼
  Kafka Consumer
  (writes to PostgreSQL)
        │
        ▼
  PostgreSQL — Raw Store
  (table: raw_transactions ~714,000 rows)
        │
        ▼
  Airflow DAG Scheduler
  (triggers every quarter @quarterly)
        │
        ▼
  Aggregation Job
  (cleans, groups by quarter + category)
        │
        ▼
  PostgreSQL — Feature Store
  (table: feature_set_quarterly — 506 rows)
        │
        ▼
  ML Application (external)
  (reads feature table to train model)
```

---

## 🧰 Tech Stack

| Tool | Version | Purpose |
|---|---|---|
| Apache Kafka | 7.4.0 | Data ingestion and messaging |
| Apache Zookeeper | 7.4.0 | Kafka coordination |
| PostgreSQL | 15 | Raw and processed data storage |
| Apache Airflow | 2.7.0 | Pipeline scheduling (@quarterly) |
| Python | 3.11 | Kafka producer, consumer, aggregation |
| Docker + Docker Compose | Latest | Infrastructure as Code (IaC) |
| GitHub | — | Version control and reproducibility |

---

## 📂 Project Structure

```
ecom-batch-pipeline/
├── docker-compose.yml              ← Starts all services (IaC)
├── data/                           ← Place Kaggle CSV files here
├── kafka/
│   ├── producer/
│   │   ├── producer.py             ← Reads CSVs, publishes to Kafka
│   │   └── Dockerfile
│   └── consumer/
│       ├── consumer.py             ← Reads Kafka, writes to PostgreSQL
│       └── Dockerfile
├── spark/
│   └── jobs/
│       ├── batch_job.py            ← PySpark aggregation job
│       └── simple_aggregate.py    ← SQL-based aggregation (fallback)
├── airflow/
│   └── dags/
│       └── ecom_quarterly_batch.py ← Airflow DAG (quarterly schedule)
├── postgres/
│   └── init/
│       └── 01_schema.sql           ← Auto-creates tables on first run
└── README.md
```

---

## ✅ Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop) installed and running
- At least **8GB RAM** available for Docker
- Internet connection (first run downloads Docker images)

---

## 📦 Dataset Setup

1. Go to: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
2. Create a free Kaggle account and download the dataset
3. Extract the zip file
4. Copy **all CSV files** into the `data/` folder

```
data/
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
├── olist_products_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_customers_dataset.csv
├── olist_sellers_dataset.csv
└── product_category_name_translation.csv
```

---

## 🚀 How to Run

### Step 1 — Clone the repository
```bash
git clone https://github.com/YOUR-USERNAME/ecom-batch-pipeline.git
cd ecom-batch-pipeline
```

### Step 2 — Start all services
```bash
docker-compose up --build
```
> First time takes 10–15 minutes. Leave this terminal running.

### Step 3 — Check all services are running
```bash
docker-compose ps
```
You should see kafka, zookeeper, postgres, airflow all showing **Up** or **healthy**.

### Step 4 — Verify data was ingested
```bash
docker exec -it postgres psql -U ecom_user -d ecom_db -c "SELECT COUNT(*) FROM raw_transactions;"
```
Expected result: **~714,000 rows** ✅

### Step 5 — Run the aggregation
```bash
docker exec -it postgres psql -U ecom_user -d ecom_db
```
Then paste this query:
```sql
INSERT INTO feature_set_quarterly (quarter, product_category, total_orders, total_revenue, avg_order_value, avg_freight_value, avg_review_score, return_rate, top_payment_type, top_seller_state, top_customer_state)
SELECT CONCAT(EXTRACT(YEAR FROM order_purchase_ts), '-Q', EXTRACT(QUARTER FROM order_purchase_ts)), product_category, COUNT(*), ROUND(SUM(price)::numeric, 2), ROUND(AVG(price)::numeric, 2), ROUND(AVG(freight_value)::numeric, 2), ROUND(AVG(review_score)::numeric, 2), ROUND(AVG(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END)::numeric, 4), MODE() WITHIN GROUP (ORDER BY payment_type), MODE() WITHIN GROUP (ORDER BY seller_state), MODE() WITHIN GROUP (ORDER BY customer_state) FROM raw_transactions WHERE order_purchase_ts IS NOT NULL GROUP BY 1, 2;
```
Then verify:
```sql
SELECT COUNT(*) FROM feature_set_quarterly;
```
Expected result: **506 rows** ✅

Then exit:
```bash
\q
```

### Step 6 — Open Airflow Dashboard
- Go to: **http://localhost:8080**
- Username: `admin` | Password: `admin`
- Find `ecom_quarterly_batch` DAG
- Turn the toggle **ON**
- Click **▶ Run**

### Step 7 — Stop everything
```bash
docker-compose down
```

---

## 📊 Pipeline Results

| Table | Rows | Description |
|---|---|---|
| `raw_transactions` | ~714,000 | All ingested e-commerce events |
| `feature_set_quarterly` | 506 | Aggregated features per quarter + category |

### Sample feature output
| Quarter | Category | Total Orders | Total Revenue | Avg Review |
|---|---|---|---|---|
| 2017-Q1 | bed_bath_table | 1,243 | R$187,432 | 4.1 |
| 2017-Q2 | health_beauty | 987 | R$142,876 | 4.3 |
| 2017-Q3 | sports_leisure | 1,102 | R$98,234 | 3.9 |

---

## 🔒 Security & Reliability

- PostgreSQL uses **role-based access control** (dedicated user `ecom_user`)
- Docker **container isolation** — if one service crashes, others keep running
- Kafka **topic partitioning** ensures no data loss during ingestion
- Airflow has **automatic retries** (2 retries per task) on failure
- Docker **volumes** persist data even if containers restart
- All credentials stored as **environment variables** (not hardcoded)

---

## ⚖️ Trade-offs

| Advantage | Disadvantage |
|---|---|
| Fully automated quarterly runs | Complex initial setup |
| Scalable — handles millions of records | Requires 8GB+ RAM locally |
| Reproducible on any machine | Spark requires extra configuration on Mac |
| Each service is independent | Multiple containers use disk space |

---

## 🎓 Course Information

- **Course:** Data Engineering (DLMDSEDE02)
- **University:** IU International University
- **Task:** Task 1 — Batch-processing data architecture
- **Dataset:** Olist Brazilian E-Commerce (Kaggle)

---

## 📄 License

This project is for educational purposes — IU International University, DLMDSEDE02.
