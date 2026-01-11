# UIDAI Data Sync

**High-Performance Aadhaar Data Intelligence Pipeline**

A production-grade backend service designed to ingest, cache, and serve monthly Aadhaar datasets (Enrolment, Biometric, Demographic) from `data.gov.in`. It provides a high-speed Insights API, leveraging Redis for L2 caching and MongoDB for raw persistence.

---

## Key Features

-   **Automated Ingestion**: Scheduled monthly sync of Enrolment, Demographic, and Biometric datasets via GitHub Actions.
-   **Smart Caching**: Multi-level caching strategy using **Redis (Upstash)** for sub-millisecond API response times.
-   **Idempotency**: Content-based hashing ensures zero duplicate records in the database.
-   **Serverless**: Optimized for **Vercel** serverless functions with fast cold-starts.
-   **Developer Experience**: Built-in Swagger-like API explorer and comprehensive documentation at `/docs`.

## Tech Stack

-   **Runtime**: Node.js 18.x (serverless-compatible)
-   **Framework**: Express.js + TypeScript
-   **Database**: MongoDB Atlas (Raw Data Storage)
-   **Caching**: Upstash Redis (L2 Cache)
-   **Deploy**: Vercel

---

## Quick Start

### 1. clone
```bash
git clone https://github.com/sreecharan-desu/uidai-data-sync.git
cd uidai-data-sync
```

### 2. Install
```bash
npm install
```

### 3. Environment
Copy `.env.example` to `.env` and configure:
```bash
cp .env.example .env
```
**Required Variables:**
- `DATA_GOV_API_KEY`: API Key from OGD Platform.
- `MONGODB_URI`: MongoDB Connection String.
- `UPSTASH_REDIS_REST_URL`: Redis URL.
- `UPSTASH_REDIS_REST_TOKEN`: Redis Token.
- `CLIENT_API_KEY`: Secret key for accessing the Insights API.

### 4. Run Locally
```bash
npm run dev
```
Explore the API at `http://localhost:3000/docs`.

---

## API Endpoints

### Insights API
**`POST /api/insights/query`**
Query cached analysis data with rich filtering.

```json
// Request
{
  "dataset": "enrolment",
  "filters": { "state": "Maharashtra" },
  "limit": 10
}
```

---

## Scheduled Ingestion
Triggered automatically on the **1st of every month** via GitHub Actions.
- **Workflow**: `.github/workflows/monthly-ingestion.yml`

## Project Structure
```
src/
├── controllers/   # Request handlers & validation
├── services/      # Core logic, caching & external API calls
├── models/        # Mongoose schemas
├── utils/         # Helpers (Logger, Redis, Transform)
└── routes/        # API Routes
```

---
**License**: ISC
