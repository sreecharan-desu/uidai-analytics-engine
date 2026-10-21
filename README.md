# uidai-analytics-engine

[![monthly raw data download](https://github.com/sreecharan-desu/uidai-analytics-engine/actions/workflows/monthly-raw-download.yml/badge.svg)](https://github.com/sreecharan-desu/uidai-analytics-engine/actions/workflows/monthly-raw-download.yml)
[![monthly clean data processing](https://github.com/sreecharan-desu/uidai-analytics-engine/actions/workflows/monthly-clean-processing.yml/badge.svg)](https://github.com/sreecharan-desu/uidai-analytics-engine/actions/workflows/monthly-clean-processing.yml)

sanchara intelligence for the uidai data hackathon 2026: turns raw aadhaar enrolment and update data into a clean, consistent dataset and serves it to power bi.

## features

- monthly etl on github actions: download raw data from api.data.gov.in, then clean and normalize it with pandas
- cleaned csvs stored in a private cloudflare r2 bucket
- fastapi checks an api key and returns a presigned download url valid for 1 hour
- power bi dashboards on top of the api

```
api.data.gov.in → github actions (raw → clean) → private r2 bucket
power bi → fastapi (api key check) → presigned r2 url
```

## stack

python 3.11 · fastapi · pandas · github actions · cloudflare r2 · power bi · docker · vercel

## run locally

```bash
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --reload       # http://localhost:8000/dashboard
docker-compose up --build           # or with docker
```

## env

copy `.env.example` to `.env`. the etl scripts need `DATA_GOV_API_KEY` and a github token.

---

built by [sreecharan desu](https://www.sreecharandesu.in) · [more projects](https://www.sreecharandesu.in/work)
