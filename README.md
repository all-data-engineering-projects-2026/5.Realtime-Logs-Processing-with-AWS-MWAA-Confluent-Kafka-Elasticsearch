# Realtime Logs Processing System

**Production-grade real-time log ingestion, streaming, and analytics pipeline**

Built with **Amazon MWAA + Confluent Cloud Kafka + Elasticsearch**

---

## 📌 Project Overview

This project implements a **scalable, secure, and observable real-time logs processing system** capable of handling high-volume website/application logs.

### Core Objectives
- Generate and produce realistic logs in real-time
- Stream logs reliably through Kafka
- Index logs into Elasticsearch for instant search and analytics
- Orchestrate everything using Amazon Managed Workflows for Apache Airflow (MWAA)
- Follow production best practices (secret management, CI/CD, monitoring)

---

## 🏗️ Architecture

```
                    ┌─────────────────────┐
                    │   GitHub Actions    │
                    │   (CI/CD Pipeline)  │
                    └──────────┬──────────┘
                               │
                               ▼
┌──────────────────────┐   ┌──────────────────────┐   ┌──────────────────────┐
│   Amazon MWAA        │   │  Confluent Cloud     │   │   Elasticsearch      │
│   (Airflow 3.2.1)    │──▶│  Kafka               │──▶│   (billion_website_  │
│                      │   │  Topic:              │   │    logs index)       │
│  • log_generation_   │   │  billion_website_    │   │                      │
│    pipeline          │   │  logs                │   │  • Real-time search  │
│  • log_consumer_     │   │                      │   │  • Analytics         │
│    pipeline          │   │                      │   │                      │
└──────────────────────┘   └──────────────────────┘   └──────────────────────┘
         │
         ▼
┌──────────────────────┐
│  AWS Secrets Manager │
│  (Kafka Credentials) │
└──────────────────────┘
```

**Key Components:**
- **Orchestration**: Amazon MWAA (Managed Airflow)
- **Streaming**: Confluent Cloud Kafka
- **Search & Analytics**: Elasticsearch
- **Secrets**: AWS Secrets Manager
- **Storage & CI/CD**: Amazon S3 + GitHub Actions

---

## 🛠️ Technology Stack

| Layer              | Technology                    | Purpose                              |
|--------------------|-------------------------------|--------------------------------------|
| Orchestration      | Amazon MWAA (Airflow 3.2.1)   | DAG scheduling, execution & monitoring |
| Message Broker     | Confluent Cloud Kafka         | High-throughput real-time streaming  |
| Search & Analytics | Elasticsearch                 | Real-time log indexing & querying    |
| Secrets Management | AWS Secrets Manager           | Secure Kafka credential storage      |
| Storage            | Amazon S3                     | DAG code storage                     |
| CI/CD              | GitHub Actions                | Automated DAG deployment             |
| Monitoring         | Amazon CloudWatch + Airflow UI| Logs, metrics & pipeline health      |

---

## 📁 Project Structure

```
mwaa_project/
├── .github/
│   └── workflows/
│       └── mwaa-s3-sync.yml          # CI/CD: Sync DAGs from GitHub → S3
├── dags/
│   ├── logs_processing_pipeline.py   # Main consumer pipeline
│   ├── logs_producer.py              # Log generation & Kafka producer
│   └── utils.py                      # Helper functions
├── logs/                             # Local log storage (if needed)
├── requirements.txt
├── airflow.cfg
└── README.md
```

---

## 🔄 How It Works (End-to-End Flow)

1. **Code Deployment**
   - Developer pushes code to GitHub
   - GitHub Actions automatically syncs DAGs to Amazon S3

2. **Log Generation Pipeline** (`log_generation_pipeline`)
   - Scheduled DAG runs every 5 minutes
   - `produce_logs` task executes
   - Fetches Kafka credentials securely from **AWS Secrets Manager**
   - Generates 200 realistic website logs
   - Publishes messages to Kafka topic `billion_website_logs`

3. **Log Consumption Pipeline** (`log_consumer_pipeline`)
   - Consumes messages from Kafka in real-time
   - Indexes documents into Elasticsearch index `billion_website_logs`

4. **Observability**
   - Full visibility in Airflow UI
   - Detailed logs in Amazon CloudWatch
   - Real-time message flow visible in Confluent Cloud & Elasticsearch

---

## ✨ Key Features & Achievements

- ✅ **Production-grade security** — No hardcoded credentials (uses AWS Secrets Manager)
- ✅ **Fully managed infrastructure** — Amazon MWAA + Confluent Cloud
- ✅ **Real-time processing** — Logs visible in Elasticsearch within seconds
- ✅ **Clean separation of concerns** — Two independent, focused DAGs
- ✅ **Automated CI/CD** — GitHub Actions → S3 → MWAA
- ✅ **Excellent observability** — Airflow + CloudWatch integration
- ✅ **Scalable architecture** — Easy to extend for higher volumes

---

## 🔐 Security Best Practices Implemented

- Kafka credentials (Bootstrap Server, SASL username/password) stored in **AWS Secrets Manager**
- Credentials fetched at runtime inside Airflow tasks
- SASL/SSL encryption enabled for Kafka communication
- No secrets in code or Git history

---

## 📊 Monitoring & Observability

- **Airflow UI**: DAG status, task logs, execution history
- **Amazon CloudWatch**: MWAA environment logs and metrics
- **Confluent Cloud**: Kafka topic throughput, consumer lag
- **Elasticsearch**: Index health, document count, search performance

---

## 🚀 Future Improvements

- Add data quality checks and validation in consumer pipeline
- Implement Index Lifecycle Management (ILM) in Elasticsearch
- Add alerting on pipeline failures via SNS / Slack
- Introduce schema registry for log structure governance
- Add real-time dashboards in Kibana

---

## 👨‍💻 Author

**Himanshu**  
Data Engineering | Azure • AWS • Databricks • Airflow • Kafka • Spark

---

## 📄 License

This project is created for learning and portfolio demonstration purposes.

---

**Built with  using modern data engineering best practices on AWS**