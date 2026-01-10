# UIDAI Daily Data Sync Service

A production-ready backend service to ingest monthly Aadhaar datasets from data.gov.in into MongoDB.

## Features

- **Automated Ingestion**: Fetches Enrolment, Demographic, and Biometric datasets via data.gov.in API.
- **Raw Storage**: Stores data exactly as received in separate MongoDB collections.
- **Idempotency**: Prevents duplicate records using content-based hashing.
- **Serverless**: Deployed on Vercel as a Serverless Function.
- **Scheduled**: GitHub Actions workflow for monthly triggers.

## Tech Stack

- Node.js 18.x
- Express.js + TypeScript
- MongoDB (Atlas)
- Vercel

## Setup

1. **Clone the repository**
2. **Install dependencies**: `npm install`
3. **Environment Setup**:
   Copy `.env.example` to `.env` and fill in the details.
   ```bash
   cp .env.example .env
   ```
   Required variables:
   - `DATA_GOV_API_KEY`: Your API key from data.gov.in
   - `MONGODB_URI`: Connection string
   - `CRON_SECRET`: Shared secret for securing the ingestion endpoint

4. **Local Development**:
   ```bash
   npm run dev
   ```
   Server runs on `http://localhost:3000`.

## Deployment (Vercel)

1. **Push to GitHub**.
2. **Import project in Vercel**.
3. **Configure Environment Variables** in Vercel project settings matching your `.env`.
4. **Deploy**.

## Scheduled Ingestion

The ingestion is triggered automatically via GitHub Actions.

- **Workflow**: `.github/workflows/monthly-ingestion.yml`
- **Schedule**: 1st of every month at 20:30 UTC (~ 02:00 IST on 2nd).
- **Secrets Required in GitHub**:
  - `VERCEL_PROJECT_PRODUCTION_URL`: The full base URL of your deployed Vercel app (e.g., `https://your-project.vercel.app`).
  - `CRON_SECRET`: Must match the one in Vercel env.

## API Endpoints

### POST `/api/ingest/monthly`

Triggers the ingestion process for all datasets.

- **Headers**: `X-CRON-SECRET: <your_secret>`
- **Response**: JSON with status of each dataset ingestion.

## Directory Structure

- `src/`: Source code
  - `config/`: Env config
  - `controllers/`: API logic
  - `models/`: Mongoose models
  - `services/`: Data fetch and ingestion logic
  - `utils/`: Helpers (Logger, etc.)
- `api/`: Vercel entry point
- `.github/`: CI/CD workflows
