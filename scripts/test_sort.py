
import requests
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("DATA_GOV_API_KEY")
RID = "65454dab-1517-40a3-ac1d-47d4dfe6891c" # Biometric

def test_sort(sort_dir):
    url = f"https://api.data.gov.in/resource/{RID}"
    params = {
        "api-key": API_KEY,
        "format": "json",
        "limit": 5,
        "sort[date]": sort_dir
    }
    resp = requests.get(url, params=params)
    data = resp.json()
    print(f"Sort [date]: {sort_dir}")
    for r in data.get('records', []):
        print(f"  {r.get('date')} - {r.get('state')}")

test_sort('asc')
test_sort('desc')
