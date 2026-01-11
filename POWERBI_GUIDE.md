# PowerBI Integration Guide

This guide explains how to connect Microsoft PowerBI to the UIDAI Data Sync API to create live dashboards.

The core service provides two main approaches:
1.  **Analytics API (`/api/analytics`)**: Returns pre-aggregated JSON summaries (State, Age, Month trends). Best for creating high-level dashboards instantly.
2.  **Raw Datasets (`/api/datasets`)**: Returns the full CSV asset via GitHub redirect. Best if you want to do custom transformations locally in PowerBI.

---

## Method 1: Using the Analytics API (Best for Dashboards)
This method connects directly to the processed insights, matching the notebook analysis.

### Steps:
1.  Open PowerBI Desktop.
2.  Click **Get Data** -> **Web**.
3.  In the URL field, paste one of the following dynamic links:

| Dataset | 2025 Analytics URL |
| :--- | :--- |
| **Enrolment** | `https://uidai.sreecharandesu.in/api/analytics/enrolment?year=2025` |
| **Biometric** | `https://uidai.sreecharandesu.in/api/analytics/biometric?year=2025` |
| **Demographic** | `https://uidai.sreecharandesu.in/api/analytics/demographic?year=2025` |

4.  Click **OK**.
5.  In the JSON response, click the **"Record"** links inside the `data` field to drill down into `by_state`, `by_age_group`, or `by_month`.
6.  Convert these records to Tables to visualize them.

*(See `POWERBI_VISUALIZATION_GUIDE.md` for a full dashboard tutorial using this method).*

---

## Method 2: Raw CSV Direct Import
Use this if you need row-level granular data (e.g. specific pincode analysis) rather than summaries.

### Steps:
1.  Open PowerBI Desktop.
2.  Click **Get Data** -> **Web**.
3.  In the URL field, paste one of the following dynamic links:

| Dataset | 2025 CSV URL | 2026 CSV URL |
| :--- | :--- | :--- |
| **Enrolment** | `https://uidai.sreecharandesu.in/api/datasets/enrolment?year=2025` | `https://uidai.sreecharandesu.in/api/datasets/enrolment?year=2026` |
| **Biometric** | `https://uidai.sreecharandesu.in/api/datasets/biometric?year=2025` | `https://uidai.sreecharandesu.in/api/datasets/biometric?year=2026` |
| **Demographic** | `https://uidai.sreecharandesu.in/api/datasets/demographic?year=2025` | `https://uidai.sreecharandesu.in/api/datasets/demographic?year=2026` |

4.  Click **OK**.
5.  PowerBI will detect the redirect to the public GitHub asset. Click **Connect** (Anonymous).
6.  Click **Load**.

### Tips:
*   **Large Files**: These CSVs can be large (100MB+). Initial load might take time.
*   **Live Data**: Clicking "Refresh" will fetch the latest daily synced file.

---

## Method 3: Advanced Filtered API (For Developers)
If you need specific slices of raw data without downloading the whole CSV (e.g., "Only Maharashtra"), use the Insights POST API.

1.  Get Data -> Blank Query.
2.  Open Advanced Editor.
3.  Use script:
```powerquery
let
    url = "https://uidai.sreecharandesu.in/api/insights/query",
    headers = [#"Content-Type"="application/json", #"X-API-KEY"="YOUR_KEY"],
    body = Json.FromValue([dataset="enrolment", filters=[state="Maharashtra"], limit=1000]),
    response = Web.Contents(url, [Headers=headers, Content=body]),
    json = Json.Document(response)
in
    json
```
