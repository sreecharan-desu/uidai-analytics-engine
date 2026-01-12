import os
import requests
import pandas as pd
import json
import re
import sys
import time
from datetime import datetime
from dotenv import load_dotenv

# Add project root to path to import app modules if needed
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.utils.state_data import STATE_STANDARD_MAP, VALID_STATES
from app.config import config

load_dotenv()

DATA_GOV_API_KEY = os.getenv("DATA_GOV_API_KEY")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

if not DATA_GOV_API_KEY:
    print("Error: DATA_GOV_API_KEY not found in environment.")
    sys.exit(1)

# Load Pincode Map
PINCODE_MAP = {}
pincode_path = os.path.join(os.getcwd(), 'app', 'config', 'pincodeMap.json')
if os.path.exists(pincode_path):
    with open(pincode_path, 'r') as f:
        PINCODE_MAP = json.load(f)

def normalize_state(raw, pincode=None):
    if not raw:
        return None
    clean_str = re.sub(r'[^a-z0-9 ]', ' ', str(raw).lower().strip())
    clean_str = re.sub(r'\s+', ' ', clean_str).strip()
    
    mapped = STATE_STANDARD_MAP.get(clean_str)
    if mapped and mapped in VALID_STATES:
        return mapped
        
    if str(pincode) in PINCODE_MAP:
        return PINCODE_MAP[str(pincode)]
        
    return None

def fetch_all_records(resource_id, dataset_name, target_year=None):
    base_url = f"https://api.data.gov.in/resource/{resource_id}"
    params = {
        "api-key": DATA_GOV_API_KEY,
        "format": "json",
        "limit": 10000,
        "offset": 0
    }
    
    if target_year:
        # Note: data.gov.in filters are exact. For dates, it might be tricky.
        # Usually, it's better to fetch large chunks and filter locally if date formats vary.
        pass

    all_records = []
    total = 1 # temporary
    offset = 0
    
    print(f"Starting sync for {dataset_name} ({resource_id})...")
    
    while offset < total:
        params["offset"] = offset
        try:
            resp = requests.get(base_url, params=params, timeout=30)
            if resp.status_code != 200:
                print(f"Error fetching data: {resp.status_code}")
                break
                
            data = resp.json()
            total = int(data.get("total", 0))
            records = data.get("records", [])
            
            if not records:
                break
                
            all_records.extend(records)
            offset += len(records)
            
            print(f"Progress: {offset}/{total} records fetched...", end="\r")
            
        except Exception as e:
            print(f"\nRequest failed: {e}")
            break
            
    print(f"\nFetched {len(all_records)} total records for {dataset_name}.")
    return all_records

def process_and_save(dataset_name, records):
    if not records:
        return
        
    df = pd.DataFrame(records)
    
    # Normalize State
    # Note: data.gov.in names can be 'state' or 'State' or 'state_name' 
    # but based on our insights_service.py we assume 'state'
    state_col = 'state' if 'state' in df.columns else ('State' if 'State' in df.columns else None)
    pincode_col = 'pincode' if 'pincode' in df.columns else None
    
    if state_col:
        df['norm_state'] = df.apply(lambda row: normalize_state(row.get(state_col), row.get(pincode_col)), axis=1)
        # Filter only valid states
        df = df[df['norm_state'].notna()]
    
    # Process Date/Year
    date_col = 'date' if 'date' in df.columns else None
    if not date_col:
        print("Error: Date column not found.")
        return

    # Extract Year safely
    def get_year(d):
        if not d: return None
        parts = re.split(r'[-/]', str(d))
        if len(parts) == 3:
            # Check for YYYY-MM-DD or DD-MM-YYYY
            if len(parts[0]) == 4: return parts[0]
            if len(parts[2]) == 4: return parts[2]
        return None

    df['year'] = df[date_col].apply(get_year)
    df = df[df['year'].notna()]
    
    # Save Full File
    output_dir = os.path.join(os.getcwd(), 'public', 'datasets')
    os.makedirs(output_dir, exist_ok=True)
    
    full_path = os.path.join(output_dir, f"{dataset_name}_full.csv")
    df.to_csv(full_path, index=False)
    print(f"Saved full dataset to {full_path}")
    
    # Save Year Specific Files
    split_dir = os.path.join(output_dir, 'split_data')
    os.makedirs(split_dir, exist_ok=True)
    
    for year, group in df.groupby('year'):
        year_path = os.path.join(split_dir, f"{dataset_name}_{year}.csv")
        group.to_csv(year_path, index=False)
        print(f"Saved {year} data to {year_path}")

def main():
    datasets = config.RESOURCES # enrolment, biometric, demographic
    
    for name, rid in datasets.items():
        records = fetch_all_records(rid, name)
        process_and_save(name, records)
        
    print("\nSync Complete!")

if __name__ == "__main__":
    main()
