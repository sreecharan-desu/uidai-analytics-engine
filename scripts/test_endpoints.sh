#!/bin/bash
BASE_URL="http://127.0.0.1:8000"
API_KEY="61c1713c4960ac786a29873e689c9407f49a45b0e6bceba893df2f7e8285230e"
SLEEP_DURATION=1

echo "Targeting Base URL: $BASE_URL"

check_status() {
    url=$1
    echo "---------------------------------------------------"
    echo "Checking $url"
    # -o /dev/null to discard body, -w to print status code
    curl -s -o /dev/null -w "Status: %{http_code}\nTime: %{time_total}s\n" "$url"
    sleep $SLEEP_DURATION
}

check_post() {
    url=$1
    data=$2
    echo "---------------------------------------------------"
    echo "Checking POST $url"
    echo "Data: $data"
    curl -s -w "\nStatus: %{http_code}\n Time: %{time_total}s\n" -X POST "$url" \
      -H "x-api-key: $API_KEY" \
      -H "Content-Type: application/json" \
      -d "$data"
    sleep $SLEEP_DURATION
}

# 1. Main Endpoints
check_status "$BASE_URL/"
check_status "$BASE_URL/dashboard"
check_status "$BASE_URL/docs"

# 2. Datasets (Redirects mostly)
# Use -L to follow redirects if we want to test the destination, but simple check is enough.
# The endpoint returns a Redirect (307/302).
check_status "$BASE_URL/api/datasets/biometric"
check_status "$BASE_URL/api/datasets/enrolment"
check_status "$BASE_URL/api/datasets/demographic"
check_status "$BASE_URL/api/datasets/invalid"

# 3. Analytics
check_status "$BASE_URL/api/analytics/biometric"
check_status "$BASE_URL/api/analytics/enrolment"
check_status "$BASE_URL/api/analytics/demographic"

# 4. Insights (POST)
check_post "$BASE_URL/api/insights/query" '{"dataset": "biometric", "limit": 5}'
check_post "$BASE_URL/api/insights/query" '{"dataset": "enrolment", "limit": 1}'

echo "---------------------------------------------------"
echo "Done."
