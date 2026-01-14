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
from app.core.config import settings
# Import centralized cleaning
from app.utils.cleaning_utils import clean_dataframe

load_dotenv()

DATA_GOV_API_KEY = os.getenv("DATA_GOV_API_KEY")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

if not DATA_GOV_API_KEY:
    print("Error: DATA_GOV_API_KEY not found in environment.")
    sys.exit(1)

# Load Pincode Map
PINCODE_MAP = {}
pincode_path = os.path.join(os.getcwd(), 'app', 'core', 'pincodeMap.json')
if os.path.exists(pincode_path):
    with open(pincode_path, 'r') as f:
        PINCODE_MAP = json.load(f)

def download_existing_from_github(dataset_name):
    """Download existing full CSV from GitHub to find the starting point."""
    # Assuming standard repo if not in settings, or add to settings. 
    # Current code uses hardcoded fallback if config attribute missing.
    repo = 'sreecharan-desu/uidai-data-sync'
    url = f"https://github.com/{repo}/releases/download/dataset-latest/{dataset_name}_full.csv"
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
    
    try:
        # Initial request to get current total and first batch
        resp = requests.get(base_url, params=params, timeout=30)
        if resp.status_code != 200:
            print(f"Error checking status for {dataset_name}: {resp.status_code}")
            return []
        
        data = resp.json()
        if data.get("status") != "ok":
            print(f"API returned error for {dataset_name}: {data.get('message')}")
            return []

        current_total = data.get("total")
        if current_total is None:
            print(f"Warning: API did not return total count for {dataset_name}. Skipping.")
            return []
            
        current_total = int(current_total)
        
        if start_offset >= current_total:
            print(f"Dataset {dataset_name} is already up to date ({start_offset}/{current_total}).")
            return []
            
        print(f"Syncing {dataset_name} ({resource_id})...")
        print(f"Found {current_total - start_offset} new records (Total: {current_total}).")
        
        records = data.get("records", [])
        all_new_records.extend(records)
        offset = start_offset + len(records)
        
        while offset < current_total:
            params["offset"] = offset
            resp = requests.get(base_url, params=params, timeout=30)
            if resp.status_code != 200:
                print(f"\nBatch fetch failed at offset {offset}")
                break
            
            data = resp.json()
            records = data.get("records", [])
            if not records: break
            
            all_new_records.extend(records)
            offset += len(records)
            print(f"Progress: {offset}/{current_total} fetched...", end="\r")

    except Exception as e:
        print(f"Incremental fetch failed: {e}")
        
    print(f"\nFetched {len(all_new_records)} new records.")
    return all_new_records

def process_and_merge(dataset_name, local_base_path, new_records):
    # Load Existing (Base)
    if local_base_path and os.path.exists(local_base_path):
        df_base = pd.read_csv(local_base_path)
    else:
        df_base = pd.DataFrame()

    df_new = pd.DataFrame()
    if new_records:
        df_new = pd.DataFrame(new_records)
        # Apply Clean to New Records
        df_new = clean_dataframe(df_new, dataset_name)
    
    if df_new.empty and not new_records:
        if df_base.empty:
            print(f"No data to process for {dataset_name}.")
            return
        # If we have base data but no new data, we might just be ensuring splitting is correct.
        df_final = df_base
    else:
        # Merge
        df_final = pd.concat([df_base, df_new], ignore_index=True)
        # Deduplicate
        # subset_cols = [c for c in ['date', 'pincode', 'state'] if c in df_final.columns]
        # if subset_cols:
        #      df_final.drop_duplicates(subset=subset_cols, keep='last', inplace=True)
        # Not strictly deduplicating here as `clean_dataframe` might affect keys
        # Generally appending is safe if offsets were correct.
    
    if df_final.empty:
        return

    # To be absolutely sure everything is clean (e.g. if base was dirty), 
    # we could run clean_dataframe on the whole thing, but that might be heavy.
    # Assuming base is clean (from reprocess script).
    
    # However, for consistency, let's ensure 'year' column exists
    if 'year' not in df_final.columns and 'date' in df_final.columns:
         # Quick year extract if missing
         def get_year(d):
            if pd.isna(d): return None
            parts = re.split(r'[-/]', str(d))
            if len(parts) == 3:
                if len(parts[0]) == 4: return parts[0]
                if len(parts[2]) == 4: return parts[2]
            return None
         df_final['year'] = df_final['date'].apply(get_year)

    # Save
    output_dir = os.path.join(os.getcwd(), 'public', 'datasets')
    os.makedirs(output_dir, exist_ok=True)
    full_path = os.path.join(output_dir, f"{dataset_name}_full.csv")
    
    # Drop year for full file if we want to matching original schema, 
    # but keeping it is useful. Current format seems to imply keeping it for splitting, dropping for save? 
    # The reprocess script drops it. Let's drop it to match.
    if 'year' in df_final.columns:
        df_final.drop(columns=['year']).to_csv(full_path, index=False)
    else:
        df_final.to_csv(full_path, index=False)
    
    split_dir = os.path.join(output_dir, 'split_data')
    os.makedirs(split_dir, exist_ok=True)
    
    if 'year' in df_final.columns:
        for year, group in df_final.groupby('year'):
            group.drop(columns=['year']).to_csv(os.path.join(split_dir, f"{dataset_name}_{year}.csv"), index=False)
    
    print(f"Update complete for {dataset_name}. Total records: {len(df_final)}")

def main():
    datasets = settings.RESOURCES
    
    for name, rid in datasets.items():
        local_file = download_existing_from_github(name)
        count = get_local_record_count(local_file)
        new_records = fetch_incremental_records(rid, name, start_offset=count)
        process_and_merge(name, local_file, new_records)
        
    print("\nIncremental Sync Complete!")

if __name__ == "__main__":
    main()
