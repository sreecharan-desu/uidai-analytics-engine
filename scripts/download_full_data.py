import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import time
from dotenv import load_dotenv

load_dotenv()

DATA_GOV_API_KEY = os.getenv("DATA_GOV_API_KEY")
if not DATA_GOV_API_KEY:
    raise ValueError("DATA_GOV_API_KEY not found in environment")

RESOURCES = {
    "enrollment": "ecd49b12-3084-4521-8f7e-ca8bf72069ba",
    "demographic": "19eac040-0b94-49fa-b239-4f2fd8677d53",
    "biometric": "65454dab-1517-40a3-ac1d-47d4dfe6891c",
}

OUTPUT_DIR = "public/datasets"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

import concurrent.futures
import subprocess
import sys

def upload_to_release(file_path):
    """
    Uploads a file to the dataset-raw release using gh cli.
    """
    if not os.path.exists(file_path):
        return
    
    print(f"Uploading {file_path} to dataset-raw release...")
    try:
        # Check if gh is available
        subprocess.run(["gh", "release", "upload", "dataset-raw", file_path, "--clobber"], check=True)
        print(f"Successfully uploaded {file_path}")
    except Exception as e:
        print(f"Failed to upload {file_path}: {e}")

def check_existing_file(file_path, expected_total):
    """
    Checks if the local file already exists and has reasonably close record count.
    Used for resuming or skipping.
    """
    if not os.path.exists(file_path):
        return False
    
    try:
        # Rough check using pandas
        # This is expensive for huge files but safe
        df = pd.read_csv(file_path, nrows=1) # check it's a valid csv
        # Count lines
        row_count = sum(1 for _ in open(file_path)) - 1 # subtracting header
        if row_count >= expected_total:
            return True
    except:
        pass
    return False

def get_session():
    """
    Creates a requests session with retry logic.
    """
    session = requests.Session()
    retry = Retry(
        total=5,
        read=5,
        connect=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def fetch_chunk(session, resource_id, offset, limit=10000, sort_order="asc"):
    """
    Fetches a chunk of data with retries. Adds sort params for stability.
    """
    url = f"https://api.data.gov.in/resource/{resource_id}"
    params = {
        "api-key": DATA_GOV_API_KEY,
        "format": "json",
        "limit": limit,
        "offset": offset,
        # Standard UIDAI fields for stable sorting
        "sort[date]": sort_order,
        "sort[state]": sort_order,
        "sort[district]": sort_order,
        "sort[pincode]": sort_order
    }
    
    max_retries = 3
    for i in range(max_retries):
        try:
            resp = session.get(url, params=params, timeout=(10, 60))
            resp.raise_for_status()
            
            data = resp.json()
            if data.get("status") == "ok":
                return data.get("records", []), data.get("total", 0)
            else:
                print(f"API Error at offset {offset}: {data.get('message', 'Unknown error')}")
                
        except Exception as e:
            print(f"Error fetching offset {offset} (Attempt {i+1}/{max_retries}): {e}")
            if i < max_retries - 1:
                time.sleep(5 * (i + 1))
            
    return [], 0

def download_resource(session, name, resource_id):
    print(f"\nStarting download for {name} ({resource_id})...")
    output_file = os.path.join(OUTPUT_DIR, f"{name}.csv")
    
    chunk_size = 10000 
    
    # Initial fetch to get total count
    records, total_count = fetch_chunk(session, resource_id, 0, chunk_size)
    if not records and total_count == 0:
        raise Exception(f"No records found for {name} or initial fetch failed.")

    print(f"Total records to fetch: {total_count}")
    
    if total_count <= 5000000:
        # Standard forward download
        df_all = []
        df_all.append(pd.DataFrame(records))
        fetched_count = len(records)
        
        current_offset = chunk_size
        while current_offset < total_count:
            chunk_records, _ = fetch_chunk(session, resource_id, current_offset, chunk_size)
            if chunk_records:
                df_all.append(pd.DataFrame(chunk_records))
                fetched_count += len(chunk_records)
                print(f"fetched {fetched_count}/{total_count}", end='\r')
                current_offset += chunk_size
            else:
                raise Exception(f"Download incomplete for {name}. Stopped at {current_offset}/{total_count}")
        
        final_df = pd.concat(df_all)
        final_df.to_csv(output_file, index=False)
    else:
        # Bi-directional download to bypass 5M offset limit
        print(f"Large dataset detected ({total_count}). Using bi-directional download...")
        
        # Part 1: First 4,000,000 records (ASC)
        df_asc = []
        df_asc.append(pd.DataFrame(records))
        fetched_asc = len(records)
        limit_asc = 4000000
        
        current_offset = chunk_size
        while current_offset < limit_asc:
            chunk_records, _ = fetch_chunk(session, resource_id, current_offset, chunk_size, sort_order="asc")
            if chunk_records:
                df_asc.append(pd.DataFrame(chunk_records))
                fetched_asc += len(chunk_records)
                print(f"Phase 1 (ASC): fetched {fetched_asc}/{total_count}", end='\r')
                current_offset += chunk_size
            else:
                break
        
        # Part 2: Remaining records from the end (DESC)
        # We fetch (Total - 4,000,000) + a small overlap to be safe
        df_desc = []
        limit_desc = total_count - limit_asc + chunk_size
        fetched_desc = 0
        current_offset = 0
        
        while current_offset < limit_desc:
            chunk_records, _ = fetch_chunk(session, resource_id, current_offset, chunk_size, sort_order="desc")
            if chunk_records:
                df_desc.append(pd.DataFrame(chunk_records))
                fetched_desc += len(chunk_records)
                print(f"Phase 2 (DESC): fetched {fetched_asc + fetched_desc}/{total_count} (Desc Offset: {current_offset})", end='\r')
                current_offset += chunk_size
            else:
                break

        print("\nMerging and de-duplicating...")
        df_1 = pd.concat(df_asc)
        df_2 = pd.concat(df_desc)
        
        # Combined
        final_df = pd.concat([df_1, df_2])
        # Sorting and de-duplicating on all columns ensures consistency
        final_df = final_df.drop_duplicates().sort_values(["date", "state", "district", "pincode"])
        
        final_df.to_csv(output_file, index=False)
        print(f"Final record count after de-duplication: {len(final_df)}")

    print(f"\nDownload complete for {name}. Saved to {output_file}")
    
    # Immediate upload after download
    # upload_to_release(output_file)


if __name__ == "__main__":
    session = get_session()
    
    # Use ThreadPoolExecutor to download resources in parallel
    # This significantly reduces total time as downloads utilize available bandwidth better
    # and don't block each other.
    import concurrent.futures
    
    failed_resources = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # Step 1: Check which files need download
        to_download = {}
        for name, rid in RESOURCES.items():
            output_file = os.path.join(OUTPUT_DIR, f"{name}.csv")
            
            # Fast check for total count
            _, total_count = fetch_chunk(session, rid, 0, 1)
            
            if check_existing_file(output_file, total_count):
                print(f"Skipping {name}: already complete ({total_count} records).")
                # Still try to upload just in case it wasn't uploaded before
                # upload_to_release(output_file)
            else:
                to_download[name] = rid

        # Step 2: Download remaining
        if to_download:
            future_to_name = {
                executor.submit(download_resource, session, name, rid): name 
                for name, rid in to_download.items()
            }
            
            for future in concurrent.futures.as_completed(future_to_name):
                name = future_to_name[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"CRITICAL: Failed to download {name}: {e}")
                    failed_resources.append(name)
    
    if failed_resources:
        print(f"Failed downloads: {', '.join(failed_resources)}")
        exit(1)
