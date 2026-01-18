# UIDAI Ecosystem Analytics API


[![12-Hourly Redis Flush](https://github.com/sreecharan-desu/uidai-analytics-engine/actions/workflows/flush-redis-12h.yml/badge.svg)](https://github.com/sreecharan-desu/uidai-analytics-engine/actions/workflows/flush-redis-12h.yml)



![GitHub Release](https://img.shields.io/github/v/release/sreecharan-desu/uidai-analytics-engine?style=for-the-badge&color=orange)
![Python](https://img.shields.io/badge/Python-3.11-blue?style=for-the-badge&logo=python)
![FastAPI](https://img.shields.io/badge/FastAPI-0.109-009688?style=for-the-badge&logo=fastapi)
![Vercel](https://img.shields.io/badge/Vercel-Serverless-black?style=for-the-badge&logo=vercel)
![Redis](https://img.shields.io/badge/Redis-Upstash-red?style=for-the-badge&logo=redis)

> **A Flagship Project by Sreecharan Desu and Team**  
> *Transforming Open Government Data into Real-Time, Actionable Intelligence.*

## Overview

The **UIDAI Ecosystem Analytics API** is a high-performance, enterprise-grade serverless application designed to process, aggregate, and visualize massive datasets related to the Aadhaar ecosystem. By leveraging **Data.gov.in**'s open APIs, this system provides granular insights into Enrolment, Biometric Updates, and Demographic changes across India.

This project demonstrates an **Elite Controller-Service-Repository Architecture**, ensuring scalability, maintainability, and sub-millisecond response times via multi-layer caching.

### Key Capabilities
- **Zero-Latency Insights**: Sub-millisecond data retrieval using **Upstash Redis (L2)** and In-Memory (L1) caching.
- **Deep Granularity**: Slice and dice data by **State, District, Age Group, and Time**.
- **Intelligent Normalization**: Uses advanced Pincode-to-State mapping to recover 99% of malformed raw data.
- **Automated Data Pipelines**: Self-healing CI/CD workflows that automatically fetch, clean, and publish new data monthly.

---

## Elite Architecture

The codebase follows a strict **Domain-Driven Design (DDD)** principle, optimized for Serverless execution:

```
├── app
│   ├── core          # Global Configuration & Security Settings
│   ├── api           # V1 Endpoints & Interface Definitions
│   ├── services      # Complex Business Logic & Aggregation Engines
│   ├── schemas       # Pydantic Data Models (Request/Response validation)
│   ├── models        # Database / Storage Entities
│   └── utils         # Shared Utilities (Logger, Redis Client)
├── scripts           # ETL Pipelines & Maintenance Jobs
└── .github           # Automated CI/CD Workflows
```

---

## Technology Stack

| Component | Technology | Rationale |
|-----------|------------|-----------|
| **Core** | **Python 3.11** | High performance & rich data ecosystem |
| **Framework** | **FastAPI** | Async-first, automatic OpenAPI docs |
| **Deployment** | **Vercel Serverless** | 100% Uptime, infinite scalability |
| **Caching** | **Upstash Redis** | Serverless-native, durable caching |
| **Data Processing** | **Pandas / NumPy** | Vectorized cleaning & aggregation |
| **CI/CD** | **GitHub Actions** | Automated data synchronization cron jobs |

---

## API Reference

### 1. Analytics Engine
Get real-time aggregated metrics.
```http
GET /api/analytics/{dataset}?year=2025&format=json
```

### 2. Deep Insights (Protected)
Execute complex queries against the raw dataset.
```http
POST /api/insights/query
x-api-key: YOUR_SECURE_KEY
```
**Body:**
```json
{
  "dataset": "biometric",
  "filters": { "state": "Maharashtra" },
  "limit": 100
}
```

---

## Getting Started

### Prerequisites
- Python 3.11+
- Redis (Local or Upstash)
- Data.gov.in API Key

### Installation

1. **Clone the Repository**
   ```bash
   git clone https://github.com/sreecharan-desu/uidai-analytics-engine.git
   cd uidai-analytics-engine
   ```

2. **Setup Environment**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Configure Variables**
   Create a `.env` file in the root directory:
   ```env
   DATA_GOV_API_KEY=your_key
   CLIENT_API_KEY=your_secret
   UPSTASH_REDIS_REST_URL=your_url
   UPSTASH_REDIS_REST_TOKEN=your_token
   ```

4. **Launch Application**
   ```bash
   uvicorn app.main:app --reload
   ```

---

## Automated Pipelines

This project is fully autonomous. 
- **Monthly Sync**: A GitHub Action wakes up on the 1st of every month, fetches new data from the government, cleans it, and updates the release artifacts.
- **Cache Warming**: A Vercel Cron pre-warms the cache daily to ensure users never hit a cold start.

---

<p align="center">
  Built with ❤️ for India's Open Data Ecosystem
</p>
