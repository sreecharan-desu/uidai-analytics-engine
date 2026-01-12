import os
import json
import csv
import re
import httpx
import tempfile
import asyncio
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
    if not x: return None
    x = str(x).lower().strip()
    x = re.sub(r'[^a-z0-9 ]', ' ', x)
    x = re.sub(r'\s+', ' ', x).strip()
    return x

def normalize_district_text(x):
    if not x: return 'Unknown'
    x = str(x).lower().strip()
    x = x.replace('*', '') # Remove asterisks first
    x = re.sub(r'[^a-z0-9 \-\(\)\.]', ' ', x) # Keep basic separators
    x = re.sub(r'\s+', ' ', x).strip()
    return x

# Request Coalescing Map
_inflight_requests = {}

async def get_aggregate_insights(dataset: str, year: str = 'all'):
    cache_key = f"agg_v8:{dataset}:{year or 'all'}" # Bumped version to invalidate old cache
    
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
        # cleanup
        _inflight_requests.pop(cache_key, None)

async def _get_aggregate_insights_logic(dataset: str, year: str, cache_key: str):
    # L1
    if cache_key in memory_cache:
        return memory_cache[cache_key]
        
    # L2
    try:
        cached = await redis_client.get(cache_key)
        if cached:
            if isinstance(cached, str):
                cached = json.loads(cached)
            memory_cache[cache_key] = cached
            return cached
    except Exception:
        pass
        
    # Determine File Source
    is_year_specific = (year and year != 'all')
    file_name = f"{dataset}_{year}.csv" if is_year_specific else f"{dataset}_full.csv"
    
    # Try Local File first (dev mode), then GitHub Release (prod)
    local_subdir = "split_data" if is_year_specific else ""
    # Checking multiple local paths for resilience
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
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
            process_path = temp_file.name
            is_temp = True
            
            async with httpx.AsyncClient(follow_redirects=True, timeout=60.0) as client:
                async with client.stream("GET", url) as response:
                    if response.status_code != 200:
                        raise ValueError(f"Failed to download dataset. Status: {response.status_code}")
                    
                    async for chunk in response.aiter_bytes():
                        temp_file.write(chunk)
            temp_file.close()
            logger.info(f"Downloaded to {process_path}")
            
        except Exception as e:
            if is_temp and os.path.exists(process_path):
                os.remove(process_path)
            raise ValueError(f"Download failed: {str(e)}")

    logger.info(f"Processing high-performance aggregation for {file_name}...")
    import pandas as pd
    import numpy as np

    try:
        # Load CSV efficiently
        df = pd.read_csv(process_path, low_memory=False)
        
        # 1. Normalize State
        # Convert state and pincode to strings
        df['state_raw'] = df['state'].astype(str)
        df['pincode'] = df['pincode'].astype(str).str.split('.').str[0] 
        
        # Apply strict cleaning regex
        df['state_cleaned'] = df['state_raw'].apply(normalize_state_text)
        
        # Map to Standard Names
        state_map_lower = {k.lower(): v for k, v in STATE_STANDARD_MAP.items()}
        df['norm_state'] = df['state_cleaned'].map(state_map_lower)
        
        # Pincode fallback for missing states
        missing_mask = df['norm_state'].isna()
        if missing_mask.any():
            df.loc[missing_mask, 'norm_state'] = df.loc[missing_mask, 'pincode'].map(PINCODE_MAP)
        
        # Drop rows where state is not in VALID_STATES
        valid_set = set(VALID_STATES)
        df = df[df['norm_state'].isin(valid_set)]
        
        result = {
            "total_updates": 0,
            "by_state": {},
            "by_age_group": {},
            "by_month": {},
            "by_district": {},
            "state_breakdown": {}
        }

        if df.empty:
            return result
            
        # 2. Normalize District (Notebook logic)
        df['district_raw'] = df['district'].astype(str)
        df['district_norm'] = df['district_raw'].apply(normalize_district_text)
        # Apply Aliases
        df['district_clean'] = df['district_norm'].replace(DISTRICT_ALIAS_MAP)
        # Final Title Case
        df['district'] = df['district_clean'].str.title()

        # 3. Extract Month from Date
        df['date'] = df['date'].astype(str)
        def extract_month(d):
            parts = re.split(r'[-/]', d)
            if len(parts) == 3:
                return parts[1] # Usually month is middle in dd-mm-yyyy or mm-dd-yyyy (dataset seems dd-mm-yyyy)
            return 'Unknown'
            
        df['month'] = df['date'].apply(extract_month)

        # 4. Calculate Counts based on dataset
        if dataset == 'biometric':
            df['count_5_17'] = pd.to_numeric(df['bio_age_5_17'], errors='coerce').fillna(0)
            df['count_18plus'] = pd.to_numeric(df['bio_age_17_'], errors='coerce').fillna(0)
            df['total'] = df['count_5_17'] + df['count_18plus']
            age_groups = {'5-17': 'count_5_17', '18+': 'count_18plus'}
        elif dataset == 'enrolment':
            df['count_0_5'] = pd.to_numeric(df['age_0_5'], errors='coerce').fillna(0)
            df['count_5_17'] = pd.to_numeric(df['age_5_17'], errors='coerce').fillna(0)
            df['count_18plus'] = pd.to_numeric(df['age_18_greater'], errors='coerce').fillna(0)
            df['total'] = df['count_0_5'] + df['count_5_17'] + df['count_18plus']
            age_groups = {'0-5': 'count_0_5', '5-17': 'count_5_17', '18+': 'count_18plus'}
        elif dataset == 'demographic':
            df['count_5_17'] = pd.to_numeric(df['demo_age_5_17'], errors='coerce').fillna(0)
            df['count_18plus'] = pd.to_numeric(df['demo_age_17_'], errors='coerce').fillna(0)
            df['total'] = df['count_5_17'] + df['count_18plus']
            age_groups = {'5-17': 'count_5_17', '18+': 'count_18plus'}
        
        # 5. Global Aggregates
        result['total_updates'] = int(df['total'].sum())
        
        # By State
        state_totals = df.groupby('norm_state')['total'].sum().to_dict()
        result['by_state'] = {k: int(v) for k, v in state_totals.items()}
        
        # By Age Group (Global)
        for label, col in age_groups.items():
            result['by_age_group'][label] = int(df[col].sum())
            
        # By Month (Global)
        month_totals = df[df['month'] != 'Unknown'].groupby('month')['total'].sum().to_dict()
        result['by_month'] = {str(k): int(v) for k, v in month_totals.items()}
        
        # 6. By District (Per State)
        dist_agg = df.groupby(['norm_state', 'district'])['total'].sum().reset_index()
        for idx, row in dist_agg.iterrows():
            st, dst, val = row['norm_state'], row['district'], row['total']
            if st not in result['by_district']: result['by_district'][st] = {}
            result['by_district'][st][dst] = int(val)
            
        # 7. Detailed State Breakdown
        for label, col in age_groups.items():
            s_age = df.groupby('norm_state')[col].sum().to_dict()
            for st, val in s_age.items():
                if st not in result['state_breakdown']: result['state_breakdown'][st] = {"by_age_group": {}, "by_month": {}}
                result['state_breakdown'][st]['by_age_group'][label] = int(val)
                
        month_breakdown = df[df['month'] != 'Unknown'].groupby(['norm_state', 'month'])['total'].sum().reset_index()
        for idx, row in month_breakdown.iterrows():
            st, mon, val = row['norm_state'], row['month'], row['total']
            if st not in result['state_breakdown']: result['state_breakdown'][st] = {"by_age_group": {}, "by_month": {}}
            result['state_breakdown'][st]['by_month'][str(mon)] = int(val)

        # Save to Cache
        try:
             await redis_client.set(cache_key, json.dumps(result), ex=86400)
             memory_cache[cache_key] = result
        except Exception as e:
             logger.warning(f"Failed to set cache: {e}")
        
        # Cleanup Temp
        if is_temp and os.path.exists(process_path):
            os.remove(process_path)
            
        return result
        
    except Exception as e:
        logger.error(f"Error processing CSV with Pandas: {e}")
        if is_temp and os.path.exists(process_path):
            os.remove(process_path)
        raise ValueError(f"Processing failed: {str(e)}")

async def prewarm_cache():
    logger.info("🔥 Pre-warming Analytics Cache...")
    datasets = ['enrolment', 'biometric', 'demographic']
    try:
        await asyncio.gather(*[get_aggregate_insights(ds, '2025') for ds in datasets])
        logger.info("✅ Cache Pre-warmed Successfully!")
    except Exception as e:
        logger.error(f"❌ Cache Pre-warming Failed {e}")
