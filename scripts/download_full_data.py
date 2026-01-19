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
    "enrolment": "ecd49b12-3084-4521-8f7e-ca8bf72069ba",
    "demographic": "19eac040-0b94-49fa-b239-4f2fd8677d53",
    "biometric": "65454dab-1517-40a3-ac1d-47d4dfe6891c",
}

OUTPUT_DIR = "public/datasets"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

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

def fetch_chunk(session, resource_id, offset, limit=5000):
    """
    Fetches a chunk of data with retries handled by the session and explicit logic.
    """
    url = f"https://api.data.gov.in/resource/{resource_id}"
    params = {
        "api-key": DATA_GOV_API_KEY,
        "format": "json",
        "limit": limit,
        "offset": offset
    }
    
    # Additional manual retries for non-standard errors (like partial reads that might pass the adapter)
    max_retries = 3
    for i in range(max_retries):
        try:
            # timeout=(connect_timeout, read_timeout)
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
                time.sleep(5 * (i + 1)) # Aggressive backoff
            
    return [], 0

def download_resource(session, name, resource_id):
    print(f"\nStarting download for {name} ({resource_id})...")
    output_file = os.path.join(OUTPUT_DIR, f"{name}.csv")
    
    # Use a smaller chunk size to reduce probability of IncompleteRead
    chunk_size = 5000 
    
    # Initial fetch to get total count
    records, total_count = fetch_chunk(session, resource_id, 0, chunk_size)
    if not records and total_count == 0:
        # We expect data for these resources. 0 means connection failure or API issue.
        raise Exception(f"No records found for {name} or initial fetch failed. Cannot proceed.")

    print(f"Total records to fetch: {total_count}")
    
    df = pd.DataFrame(records)
    df.to_csv(output_file, index=False)
    fetched_count = len(records)
    print(f"fetched {fetched_count}/{total_count}")

    # Start loop for remaining data
    current_offset = chunk_size
    
    # Keep track of columns to ensure consistency
    msg_cols = df.columns
    
    with open(output_file, 'a') as f:
        while current_offset < total_count:
            chunk_records, _ = fetch_chunk(session, resource_id, current_offset, chunk_size)
            
            if chunk_records:
                df_chunk = pd.DataFrame(chunk_records)
                
                # Align columns
                df_chunk = df_chunk.reindex(columns=msg_cols)
                
                df_chunk.to_csv(f, header=False, index=False)
                fetched_count += len(chunk_records)
                print(f"fetched {fetched_count}/{total_count}", end='\r')
                
                current_offset += chunk_size
            else:
                print(f"\nFailed to fetch chunk at offset {current_offset}. Stopping download for {name} to avoid gaps.")
                # We stop here. Better partial data than corrupted/gapped data? 
                # Or maybe we should retry indefinitely? 
                # Given the "monthly" nature, we probably want to fail hard if we can't get it all, 
                # but for now, exiting breaks the loop.
                # To ensure the workflow fails, we might want to raise an exception.
                raise Exception(f"Download incomplete for {name}. Stopped at {current_offset}/{total_count}")

    print(f"\nDownload complete for {name}. Saved to {output_file}")


if __name__ == "__main__":
    session = get_session()
    
    # Use ThreadPoolExecutor to download resources in parallel
    # This significantly reduces total time as downloads utilize available bandwidth better
    # and don't block each other.
    import concurrent.futures
    
    failed_resources = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # Create a future for each download
        future_to_name = {
            executor.submit(download_resource, session, name, rid): name 
            for name, rid in RESOURCES.items()
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
