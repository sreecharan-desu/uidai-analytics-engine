import os
import json
import csv
import re
import httpx
import tempfile
import asyncio
import time
import glob
from app.utils.logger import get_logger
from app.utils.redis_client import redis_client
from app.utils.state_data import STATE_STANDARD_MAP, VALID_STATES, LOWER_CASE_VALID_STATES, DISTRICT_ALIAS_MAP

logger = get_logger()

# Load Pincode Map
PINCODE_MAP = {}
possible_paths = [
    os.path.join(os.getcwd(), 'src', 'config', 'pincodeMap.json'),
    os.path.join(os.getcwd(), 'app', 'config', 'pincodeMap.json')
]

for p in possible_paths:
    if os.path.exists(p):
        try:
            with open(p, 'r') as f:
                PINCODE_MAP = json.load(f)
            logger.info(f"Loaded Pincode Map from {p}")
            break
        except Exception as e:
            logger.warning(f"Failed to load Pincode Map: {e}")

memory_cache = {}

GITHUB_REPO = 'sreecharan-desu/uidai-data-sync'
RELEASE_TAG = 'dataset-latest'

def normalize_state_text(x):
    if not isinstance(x, str): return None
    x = x.lower().strip()
    x = re.sub(r'[^a-z0-9 ]', ' ', x)
    x = re.sub(r'\s+', ' ', x).strip()
    return x

def normalize_district_text(x):
    if not isinstance(x, str): return 'Unknown'
    x = x.lower().strip()
    x = x.replace('*', '') # Remove asterisks first
    x = re.sub(r'[^a-z0-9 \-\(\)\.]', ' ', x) # Keep basic separators
    x = re.sub(r'\s+', ' ', x).strip()
    return x

def clean_stale_temp_files():
    """Aggressively clean up stale CSV files in /tmp to prevent disk full errors."""
    try:
        temp_dir = tempfile.gettempdir()
        # Find all csv files created by us (or just all csvs to be safe in this context)
        # We assume our files usually have standard naming or random temp names.
        # Let's target files older than 10 minutes.
        now = time.time()
        for f in glob.glob(os.path.join(temp_dir, "*.csv")):
            try:
                if os.stat(f).st_mtime < now - 600: # 10 mins
                    os.remove(f)
                    logger.info(f"Cleaned stale temp file: {f}")
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Failed to clean temp dir: {e}")

# Request Coalescing Map
_inflight_requests = {}

async def get_aggregate_insights(dataset: str, year: str = 'all'):
    cache_key = f"agg_v9:{dataset}:{year or 'all'}" # Bumped to v9 (Chunking Refactor)
    
    # Check In-flight
    if cache_key in _inflight_requests:
        return await _inflight_requests[cache_key]
        
    # Start new task
    task = asyncio.create_task(_get_aggregate_insights_logic(dataset, year, cache_key))
    _inflight_requests[cache_key] = task
    
    try:
        data = await task
        return data
    finally:
        _inflight_requests.pop(cache_key, None)

async def _get_aggregate_insights_logic(dataset: str, year: str, cache_key: str):
    # L1 Cache
    if cache_key in memory_cache:
        return memory_cache[cache_key]
        
    # L2 Cache
    try:
        cached = await redis_client.get(cache_key)
        if cached:
            if isinstance(cached, str):
                cached = json.loads(cached)
            memory_cache[cache_key] = cached
            return cached
    except Exception:
        pass

    # Ensure disk space is available
    clean_stale_temp_files()
        
    # Determine File Source
    is_year_specific = (year and year != 'all')
    file_name = f"{dataset}_{year}.csv" if is_year_specific else f"{dataset}_full.csv"
    
    # Local check (Dev / Pre-downloaded)
    local_subdir = "split_data" if is_year_specific else ""
    possible_local_paths = [
        os.path.join(os.getcwd(), 'public', 'datasets', local_subdir, file_name),
        os.path.join(os.getcwd(), '..', 'public', 'datasets', local_subdir, file_name) 
    ]
    
    process_path = ""
    is_temp = False
    
    found = False
    for lp in possible_local_paths:
        if os.path.exists(lp):
            logger.info(f"Using local file: {lp}")
            process_path = lp
            found = True
            break

    if not found:
        # Download from GitHub Release to /tmp
        url = f"https://github.com/{GITHUB_REPO}/releases/download/{RELEASE_TAG}/{file_name}"
        logger.info(f"Downloading from GitHub: {url}")
        
        try:
            # Use delete=False so we can close and re-open in Pandas
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
            process_path = temp_file.name
            is_temp = True
            
            async with httpx.AsyncClient(follow_redirects=True, timeout=120.0) as client:
                async with client.stream("GET", url) as response:
                    if response.status_code != 200:
                        raise ValueError(f"Failed to download dataset. Status: {response.status_code}")
                    
                    async for chunk in response.aiter_bytes():
                        temp_file.write(chunk)
            temp_file.close()
            logger.info(f"Downloaded to {process_path}")
            
        except Exception as e:
            if is_temp and os.path.exists(process_path):
                try: os.remove(process_path)
                except: pass
            raise ValueError(f"Download failed: {str(e)}")

    logger.info(f"Processing aggregation with CHUNKING for {file_name}...")
    import pandas as pd
    import numpy as np

    # Initialize Accumulators
    final_result = {
        "total_updates": 0,
        "by_state": {},
        "by_age_group": {},
        "by_month": {},
        "by_district": {},
        "state_breakdown": {} 
    }
    
    # Pre-compute state map for speed
    state_map_lower = {k.lower(): v for k, v in STATE_STANDARD_MAP.items()}
    valid_set = set(VALID_STATES)

    try:
        chunk_size = 50000 
        # Read in chunks
        for chunk in pd.read_csv(process_path, chunksize=chunk_size, low_memory=False):
            
            # --- 1. Normalize State ---
            chunk['state_raw'] = chunk['state'].astype(str)
            chunk['pincode'] = chunk['pincode'].astype(str).str.split('.').str[0]
            
            chunk['state_cleaned'] = chunk['state_raw'].apply(normalize_state_text)
            chunk['norm_state'] = chunk['state_cleaned'].map(state_map_lower)
            
            # Fallback
            missing_mask = chunk['norm_state'].isna()
            if missing_mask.any():
                chunk.loc[missing_mask, 'norm_state'] = chunk.loc[missing_mask, 'pincode'].map(PINCODE_MAP)
            
            # Filter
            chunk = chunk[chunk['norm_state'].isin(valid_set)]
            if chunk.empty:
                continue

            # --- 2. Normalize District ---
            chunk['district_raw'] = chunk['district'].astype(str)
            chunk['district_norm'] = chunk['district_raw'].apply(normalize_district_text)
            chunk['district_clean'] = chunk['district_norm'].replace(DISTRICT_ALIAS_MAP)
            chunk['district'] = chunk['district_clean'].str.title()
            
            # --- 3. Extract Month ---
            chunk['date'] = chunk['date'].astype(str)
            # vectorized month extraction is hard with regex split, utilize apply for now or optimized string slicing
            # Assuming standard formats, month is usually bits
            def extract_month_fast(s):
                # optimized: finding separators
                if '-' in s: parts = s.split('-')
                elif '/' in s: parts = s.split('/')
                else: return 'Unknown'
                return parts[1] if len(parts) == 3 else 'Unknown'

            chunk['month'] = chunk['date'].apply(extract_month_fast)

            # --- 4. Calculate Columns ---
            if dataset == 'biometric':
                chunk['c_5_17'] = pd.to_numeric(chunk['bio_age_5_17'], errors='coerce').fillna(0)
                chunk['c_18plus'] = pd.to_numeric(chunk['bio_age_17_'], errors='coerce').fillna(0)
                chunk['total'] = chunk['c_5_17'] + chunk['c_18plus']
                age_map = {'5-17': 'c_5_17', '18+': 'c_18plus'}
            elif dataset == 'enrolment':
                chunk['c_0_5'] = pd.to_numeric(chunk['age_0_5'], errors='coerce').fillna(0)
                chunk['c_5_17'] = pd.to_numeric(chunk['age_5_17'], errors='coerce').fillna(0)
                chunk['c_18plus'] = pd.to_numeric(chunk['age_18_greater'], errors='coerce').fillna(0)
                chunk['total'] = chunk['c_0_5'] + chunk['c_5_17'] + chunk['c_18plus']
                age_map = {'0-5': 'c_0_5', '5-17': 'c_5_17', '18+': 'c_18plus'}
            else: # demographic
                chunk['c_5_17'] = pd.to_numeric(chunk['demo_age_5_17'], errors='coerce').fillna(0)
                chunk['c_18plus'] = pd.to_numeric(chunk['demo_age_17_'], errors='coerce').fillna(0)
                chunk['total'] = chunk['c_5_17'] + chunk['c_18plus']
                age_map = {'5-17': 'c_5_17', '18+': 'c_18plus'}

            # --- 5. Aggregate Chunk ---
            # Total
            final_result['total_updates'] += int(chunk['total'].sum())

            # By State
            st_sums = chunk.groupby('norm_state')['total'].sum()
            for s, v in st_sums.items():
                final_result['by_state'][s] = final_result['by_state'].get(s, 0) + int(v)

            # By Age Group
            for label, col in age_map.items():
                val = int(chunk[col].sum())
                final_result['by_age_group'][label] = final_result['by_age_group'].get(label, 0) + val
            
            # By Month
            mon_sums = chunk[chunk['month'] != 'Unknown'].groupby('month')['total'].sum()
            for m, v in mon_sums.items():
                m_str = str(m)
                final_result['by_month'][m_str] = final_result['by_month'].get(m_str, 0) + int(v)

            # By District (Nested)
            dist_sums = chunk.groupby(['norm_state', 'district'])['total'].sum()
            for (st, dst), v in dist_sums.items():
                if st not in final_result['by_district']: final_result['by_district'][st] = {}
                final_result['by_district'][st][dst] = final_result['by_district'][st].get(dst, 0) + int(v)

            # State Breakdown: Age
            for label, col in age_map.items():
                s_age_sums = chunk.groupby('norm_state')[col].sum()
                for st, val in s_age_sums.items():
                    if st not in final_result['state_breakdown']: 
                        final_result['state_breakdown'][st] = {"by_age_group": {}, "by_month": {}}
                    
                    prev = final_result['state_breakdown'][st]["by_age_group"].get(label, 0)
                    final_result['state_breakdown'][st]["by_age_group"][label] = prev + int(val)
            
            # State Breakdown: Month
            s_mon_sums = chunk[chunk['month'] != 'Unknown'].groupby(['norm_state', 'month'])['total'].sum()
            for (st, mon), val in s_mon_sums.items():
                m_str = str(mon)
                if st not in final_result['state_breakdown']: 
                     final_result['state_breakdown'][st] = {"by_age_group": {}, "by_month": {}}
                
                prev = final_result['state_breakdown'][st]["by_month"].get(m_str, 0)
                final_result['state_breakdown'][st]["by_month"][m_str] = prev + int(val)

        # Save to Cache
        try:
             await redis_client.set(cache_key, json.dumps(final_result), ex=86400)
             memory_cache[cache_key] = final_result
        except Exception as e:
             logger.warning(f"Failed to set cache: {e}")
        
        return final_result
        
    except Exception as e:
        logger.error(f"Error processing CSV Chunk: {e}")
        raise ValueError(f"Processing failed: {str(e)}")
    finally:
        # Cleanup Temp ALWAYS
        if is_temp and os.path.exists(process_path):
            try: os.remove(process_path)
            except: pass


async def prewarm_cache():
    logger.info("🔥 Pre-warming Analytics Cache...")
    datasets = ['enrolment', 'biometric', 'demographic']
    try:
        await asyncio.gather(*[get_aggregate_insights(ds, '2025') for ds in datasets])
        logger.info("✅ Cache Pre-warmed Successfully!")
    except Exception as e:
        logger.error(f"❌ Cache Pre-warming Failed {e}")
