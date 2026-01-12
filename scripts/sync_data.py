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

def download_existing_from_github(dataset_name):
    """Download existing full CSV from GitHub to find the starting point."""
    url = f"https://github.com/{config.github_repo if hasattr(config, 'github_repo') else 'sreecharan-desu/uidai-data-sync'}/releases/download/dataset-latest/{dataset_name}_full.csv"
    local_path = os.path.join(os.getcwd(), 'public', 'datasets', f"{dataset_name}_full.csv")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    print(f"Checking for existing {dataset_name} on GitHub...")
    try:
        resp = requests.get(url, stream=True, timeout=10)
        if resp.status_code == 200:
            with open(local_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded existing base for {dataset_name}")
            return local_path
    except Exception as e:
        print(f"No existing dataset found or download failed: {e}")
    return None

def get_local_record_count(file_path):
    if not file_path or not os.path.exists(file_path):
        return 0
    try:
        # Fast line count for large CSVs
        with open(file_path, 'rb') as f:
            lines = sum(1 for _ in f)
        return max(0, lines - 1) # Subtract header
    except:
        return 0

def fetch_incremental_records(resource_id, dataset_name, start_offset=0):
    base_url = f"https://api.data.gov.in/resource/{resource_id}"
    params = {
        "api-key": DATA_GOV_API_KEY,
        "format": "json",
        "limit": 10000,
        "offset": start_offset
    }
    
    all_new_records = []
    
    # Initial request to get current total
    try:
        resp = requests.get(base_url, params=params, timeout=30)
        if resp.status_code != 200:
            print(f"Error checking status: {resp.status_code}")
            return []
        
        data = resp.json()
        current_total = int(data.get("total", 0))
        
        if start_offset >= current_total:
            print(f"Dataset {dataset_name} is already up to date ({start_offset}/{current_total}).")
            return []
            
        print(f"Syncing {current_total - start_offset} new records for {dataset_name} (Total: {current_total})...")
        
        # We already have the first batch from the status check
        records = data.get("records", [])
        all_new_records.extend(records)
        offset = start_offset + len(records)
        
        while offset < current_total:
            params["offset"] = offset
            resp = requests.get(base_url, params=params, timeout=30)
            if resp.status_code != 200: break
            
            data = resp.json()
            records = data.get("records", [])
            if not records: break
            
            all_new_records.extend(records)
            offset += len(records)
            print(f"Progress: {offset}/{current_total} fetched...", end="\r")
            
            if os.getenv("TEST_MODE") == "true": break

    except Exception as e:
        print(f"Incremental fetch failed: {e}")
        
    print(f"\nFetched {len(all_new_records)} new records.")
    return all_new_records

def process_and_merge(dataset_name, local_base_path, new_records):
    if not new_records and local_base_path:
        # If no new records, we might still need to split existing if they aren't split
        print(f"No new records to merge for {dataset_name}.")
        return

    # Load Existing
    if local_base_path and os.path.exists(local_base_path):
        df_base = pd.read_csv(local_base_path)
    else:
        df_base = pd.DataFrame()

    # Process New
    if new_records:
        df_new = pd.DataFrame(new_records)
        
        # Normalize and find columns (reusing logic but simplified)
        state_col = 'state' if 'state' in df_new.columns else ('State' if 'State' in df_new.columns else None)
        pincode_col = 'pincode' if 'pincode' in df_new.columns else None
        date_col = 'date' if 'date' in df_new.columns else None

        if state_col:
            df_new['norm_state'] = df_new.apply(lambda row: normalize_state(row.get(state_col), row.get(pincode_col)), axis=1)
            df_new = df_new[df_new['norm_state'].notna()]
        
        if date_col:
            def get_year(d):
                if not d: return None
                parts = re.split(r'[-/]', str(d))
                if len(parts) == 3:
                    if len(parts[0]) == 4: return parts[0]
                    if len(parts[2]) == 4: return parts[2]
                return None
            df_new['year'] = df_new[date_col].apply(get_year)
            df_new = df_new[df_new['year'].notna()]

        # Merge
        df_final = pd.concat([df_base, df_new], ignore_index=True)
        # Deduplicate just in case (optional but safe)
        if 'date' in df_final.columns and 'pincode' in df_final.columns:
             df_final.drop_duplicates(subset=[date_col, 'pincode', state_col], keep='last', inplace=True)
    else:
        df_final = df_base

    # Save and Split
    output_dir = os.path.join(os.getcwd(), 'public', 'datasets')
    os.makedirs(output_dir, exist_ok=True)
    full_path = os.path.join(output_dir, f"{dataset_name}_full.csv")
    df_final.to_csv(full_path, index=False)
    
    split_dir = os.path.join(output_dir, 'split_data')
    os.makedirs(split_dir, exist_ok=True)
    for year, group in df_final.groupby('year'):
        group.to_csv(os.path.join(split_dir, f"{dataset_name}_{year}.csv"), index=False)
    
    print(f"Update complete for {dataset_name}. Total records: {len(df_final)}")

def main():
    datasets = config.RESOURCES
    
    for name, rid in datasets.items():
        local_file = download_existing_from_github(name)
        count = get_local_record_count(local_file)
        new_records = fetch_incremental_records(rid, name, start_offset=count)
        process_and_merge(name, local_file, new_records)
        
    print("\nIncremental Sync Complete!")

if __name__ == "__main__":
    main()
