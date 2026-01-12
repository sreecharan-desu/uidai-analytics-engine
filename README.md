# UIDAI Ecosystem Analytics API

A high-performance, serverless API for visualizing Aadhaar ecosystem metadata (Enrolment, Biometric Updates, Demographic Updates). Built with **Python FastAPI** and **Vercel Serverless Functions**.

## Overview

This API aggregates millions of rows of open government data to provide real-time insights into the UIDAI ecosystem. It mirrors the official breakdown but adds granular slicing capabilities (State, District, Age Group, Monthly Trends) that are not easily accessible via raw CSVs.

### Key Features
- **High Performance**: Uses Upstash Redis (L2) and In-Memory caching (L1) for sub-millisecond response times.
- **Granular Slicing**: Filter data by State, District, Dataset Type, and Year.
- **Smart Recovery**: Automatically recovers valid State data from messy raw inputs using a Pincode fallback map.
- **Streaming Export**: Supports CSV export for integration with Looker Studio or PowerBI.

---

## Tech Stack

- **Runtime**: Python 3.11 (Vercel Serverless)
- **Framework**: FastAPI (Async)
- **Data Processing**: Native CSV Streaming (Memory Optimized)
- **Caching**: Upstash Redis (Serverless-friendly HTTP client)
- **Frontend**: Clean HTML5 Dashboard (Served statically)

---

## API Endpoints

### 1. Analytics (Public)
Get aggregated metrics for dashboards.
```http
GET /api/analytics/{dataset}?year=2025
```
- **Methods**: `GET`
- **Params**: 
  - `dataset`: `enrolment` | `biometric` | `demographic`
  - `year`: `2025` or `all`
  - `format`: `json` (default) or `csv`
  - `view`: `state` | `age` (for CSV export)

### 2. Insights Query (Protected)
Flexible query interface for raw record retrieval.
```http
POST /api/insights/query
Headers: x-api-key: <CLIENT_API_KEY>
```
- **Body**: `{ "dataset": "biometric", "filters": {"state": "Goa"}, "limit": 50 }`

### 3. Dataset Download (Public)
Redirects to the formatted CSV file hosted on GitHub Releases.
```http
GET /api/datasets/{dataset}?year=2025
```

---

## Environment Variables

Required for local development and deployment:

| Variable | Description |
|----------|-------------|
| `DATA_GOV_API_KEY` | API Key for Open Govt Data Platform (OGD) |
| `CLIENT_API_KEY` | Secret key for protecting internal query endpoints |
| `UPSTASH_REDIS_REST_URL` | Upstash Redis REST URL |
| `UPSTASH_REDIS_REST_TOKEN` | Upstash Redis REST Token |
| `NODE_ENV` | `development` or `production` |

---

## Local Development

1. **Clone & Setup**
   ```bash
   git clone <repo_url>
   cd uidai-data-sync
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Configure Environment**
   Create a `.env` file with the variables listed above.

3. **Run Server**
   ```bash
   uvicorn app.main:app --reload
   ```
   Access Dashboard: `http://localhost:8000/dashboard`

---

## Deployment

This project is optimized for **Vercel**.

1. **Install Vercel CLI**
   ```bash
   npm i -g vercel
   ```

2. **Deploy**
   ```bash
   vercel
   ```
   *Vercel will automatically detect `api/index.py` and `vercel.json` configuration.*

---

## Automated Data Sync

The project is configured to stay up-to-date with **Data.gov.in** automatically.

- **Sync Script**: `scripts/sync_data.py` pulls the latest records, normalizes them using the Pincode-State map, and generates fresh CSVs.
- **Monthly Sync (Cron)**: A GitHub Action (`.github/workflows/monthly-sync.yml`) runs on the **1st of every month**. It updates the `dataset-latest` release with the newest data.
- **Manual Trigger**: You can trigger the sync manually from the **Actions** tab in GitHub.

**Note**: Ensure `DATA_GOV_API_KEY` is added as a **GitHub Secret** in your repository settings.
