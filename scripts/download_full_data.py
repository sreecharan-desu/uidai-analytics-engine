import os
import requests
import pandas as pd
import math
import time
from dotenv import load_dotenv
import concurrent.futures

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

def fetch_chunk(resource_id, offset, limit=10000):
    url = f"https://api.data.gov.in/resource/{resource_id}"
    params = {
        "api-key": DATA_GOV_API_KEY,
        "format": "json",
        "limit": limit,
        "offset": offset
    }
    try:
        resp = requests.get(url, params=params, timeout=60)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "ok":
                return data.get("records", []), data.get("total", 0)
    except Exception as e:
        print(f"Error fetching offset {offset}: {e}")
    return [], 0

def download_resource(name, resource_id):
    print(f"\nStarting download for {name} ({resource_id})...")
    output_file = os.path.join(OUTPUT_DIR, f"{name}.csv")
    
    # Initial fetch to get total count
    records, total_count = fetch_chunk(resource_id, 0, 1)
    if total_count == 0:
        print(f"No records found for {name}.")
        return

    print(f"Total records to fetch: {total_count}")
    
    # Process in chunks
    chunk_size = 50000
    
    # First chunk to setup file
    records, _ = fetch_chunk(resource_id, 0, chunk_size)
    if not records:
        print("Failed to fetch first chunk.")
        return

    df = pd.DataFrame(records)
    df.to_csv(output_file, index=False)
    fetched_count = len(records)
    print(f"fetched {fetched_count}/{total_count}")

    # Subsequent chunks
    offsets = range(fetched_count, total_count, chunk_size)
    
    with open(output_file, 'a') as f:
        for offset in offsets:
            chunk_records, _ = fetch_chunk(resource_id, offset, chunk_size)
            if chunk_records:
                df_chunk = pd.DataFrame(chunk_records)
                # Align columns
                for col in df.columns:
                    if col not in df_chunk.columns:
                        df_chunk[col] = None
                df_chunk = df_chunk[df.columns]
                
                df_chunk.to_csv(f, header=False, index=False)
                fetched_count += len(chunk_records)
                print(f"fetched {fetched_count}/{total_count}", end='\r')
            else:
                print(f"Failed or empty chunk at offset {offset}")
                time.sleep(1)
                chunk_records, _ = fetch_chunk(resource_id, offset, chunk_size)
                if chunk_records:
                     df_chunk = pd.DataFrame(chunk_records)
                     df_chunk = df_chunk.reindex(columns=df.columns)
                     df_chunk.to_csv(f, header=False, index=False)
                     fetched_count += len(chunk_records)
                     print(f"fetched {fetched_count}/{total_count}", end='\r')


    print(f"\nDownload complete for {name}. Saved to {output_file}")


if __name__ == "__main__":
    for name, rid in RESOURCES.items():
        download_resource(name, rid)
