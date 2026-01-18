
import requests
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("DATA_GOV_API_KEY")
RID = "65454dab-1517-40a3-ac1d-47d4dfe6891c" # Biometric

def test_query(query_str):
    url = f"https://api.data.gov.in/resource/{RID}"
    params = {
        "api-key": API_KEY,
        "format": "json",
        "limit": 1,
        "query": query_str
    }
    resp = requests.get(url, params=params)
    data = resp.json()
    print(f"Query: {query_str} -> Total: {data.get('total')}")
    if data.get('records'):
        print(f"Sample Record Date: {data['records'][0].get('date')}")

def test_filter(date_val):
    url = f"https://api.data.gov.in/resource/{RID}"
    params = {
        "api-key": API_KEY,
        "format": "json",
        "limit": 1,
        "filters[date]": date_val
    }
    resp = requests.get(url, params=params)
    data = resp.json()
    print(f"Filter [date]: {date_val} -> Total: {data.get('total')}")

test_filter('03-01-2026')
