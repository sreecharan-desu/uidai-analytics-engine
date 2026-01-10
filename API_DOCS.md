# Admin Insights API Documentation

This API endpoint allows the admin dashboard to query the raw ingested datasets (Enrolment, Demographic, Biometric) for real-time insights.

## Endpoint

**POST** `https://uidai.sreecharandesu.in/api/insights/query`

## Request Headers

- `Content-Type`: `application/json`

## Request Body

| Field     | Type   | Required | Description                                                                 |
| :-------- | :----- | :------- | :-------------------------------------------------------------------------- |
| `dataset` | String | **Yes**  | One of: `"enrolment"`, `"demographic"`, `"biometric"`                      |
| `page`    | Number | No       | Pagination page number (default: 1)                                         |
| `limit`   | Number | No       | Number of records per page (default: 100, max: 1000)                        |
| `filters` | Object | No       | Key-value pairs to filter data. Must match exact field names in raw record. |

### Example Request

```json
{
  "dataset": "enrolment",
  "page": 1,
  "limit": 50,
  "filters": {
    "District": "Hyderabad",
    "State": "Telangana"
  }
}
```

## Response Body

The response contains metadata about the query and the array of matching records.

### Example Success Response (200 OK)

```json
{
  "meta": {
    "dataset": "enrolment",
    "total": 1542,
    "page": 1,
    "limit": 50,
    "founded": 50
  },
  "data": [
    {
      "_id": "65ae...",
      "resource_id": "ecd49b...",
      "ingestion_timestamp": "2024-01-10T12:00:00Z",
      "source": "data.gov.in",
      "State": "Telangana",
      "District": "Hyderabad",
      "Enrolment_Agencies": "CSC e-Governance Services India Limited",
      "Enrolment_Count": "45"
      // ... dynamic fields from the raw dataset
    },
    // ... more records
  ]
}
```

## Error Responses

- **400 Bad Request**: Missing or invalid `dataset` parameter.
- **500 Internal Server Error**: Database connectivity or processing issues.
