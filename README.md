# E-Commerce Batch Processing Pipeline
### DLMDSEDE02 — Data Engineering Portfolio | IU International University

---

## What This Project Does

This project builds a **batch-processing data pipeline** for an e-commerce machine learning application.
It ingests the Olist Brazilian E-Commerce dataset (~1M records), stores it in PostgreSQL,
and every quarter Apache Airflow triggers a Spark job that cleans and aggregates the data
into a feature table ready for an ML model.

---

## Architecture

```
Olist CSV Files (Kaggle)
        │
        ▼
  Kafka Producer  ──►  Kafka Broker (topic: ecom_transactions)  ──►  Kafka Consumer
                                                                            │
                                                                            ▼
                                                                    PostgreSQL (raw_transactions)
                                                                            │
                                                              Airflow DAG (@quarterly)
                                                                            │
                                                                            ▼
                                                                    Spark Batch Job
                                                                            │
                                                                            ▼
                                                              PostgreSQL (feature_set_quarterly)
                                                                            │
                                                                            ▼
                                                                    ML Application
```

All components run as **isolated Docker microservices** via Docker Compose (Infrastructure as Code).

---

## Project Structure

```
ecom-batch-pipeline/
├── docker-compose.yml            ← Main IaC file — starts all services
├── data/                         ← Place Olist CSV files here (not in Git)
├── kafka/
│   ├── producer/
│   │   ├── producer.py           ← Reads CSVs, publishes to Kafka
│   │   └── Dockerfile
│   └── consumer/
│       ├── consumer.py           ← Reads Kafka, writes to PostgreSQL
│       └── Dockerfile
├── spark/
│   └── jobs/
│       └── batch_job.py          ← PySpark: clean, aggregate, write features
├── airflow/
│   └── dags/
│       └── ecom_quarterly_batch.py ← Airflow DAG: quarterly schedule
├── postgres/
│   └── init/
│       └── 01_schema.sql         ← Auto-runs on first DB start
└── docs/
    └── CLAUDE_CODE_GUIDE.md      ← Step-by-step Claude Code prompts
```

---

## Prerequisites

- Docker Desktop installed and running
- At least 8GB RAM available for Docker
- Python 3.11+ (for local testing only)
- Git

---

## Dataset Setup

1. Go to [https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
2. Download all CSV files
3. Place them inside the `data/` folder in this project

---

## How to Run

```bash
# 1. Clone / open the project
cd ecom-batch-pipeline

# 2. Build and start all services
docker-compose up --build

# 3. Check services are running
docker-compose ps

# 4. Open Airflow UI
# → http://localhost:8080
# → Username: admin | Password: admin

# 5. Trigger DAG manually (or wait for quarterly schedule)
# → Find "ecom_quarterly_batch" in Airflow and press ▶ Run

# 6. Stop everything
docker-compose down
```

---

## Services & Ports

| Service    | Port  | Description                        |
|------------|-------|------------------------------------|
| Kafka      | 9092  | Message broker                     |
| PostgreSQL | 5432  | Raw + processed data storage       |
| Airflow    | 8080  | DAG scheduler UI                   |
| Spark      | 8081  | Batch processing engine            |

---

## Security Measures

- PostgreSQL uses role-based access (dedicated user `ecom_user`)
- Credentials stored as Docker environment variables (use Docker secrets in production)
- Docker volumes ensure data persistence across restarts
- Container isolation provides fault boundary between services

---

## License

This project is for educational purposes — IU International University, DLMDSEDE02.
