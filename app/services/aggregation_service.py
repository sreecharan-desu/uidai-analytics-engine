import os
import json
import csv
import re
import httpx
import tempfile
import asyncio
from app.utils.logger import get_logger
from app.utils.redis_client import redis_client
from app.utils.state_data import STATE_STANDARD_MAP, VALID_STATES, LOWER_CASE_VALID_STATES

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

def normalize_state(raw, pincode=None):
    if not raw:
        return None
    
    clean_str = re.sub(r'[^a-z0-9 ]', ' ', raw.lower().strip())
    clean_str = re.sub(r'\s+', ' ', clean_str).strip()
    
    mapped = STATE_STANDARD_MAP.get(clean_str)
    if mapped and mapped in VALID_STATES:
        return mapped
        
    if pincode and pincode in PINCODE_MAP:
        return PINCODE_MAP[pincode]
        
    if not mapped:
        mapped = clean_str.title() 
        
    if mapped in VALID_STATES:
        return mapped
        
    lower_map = LOWER_CASE_VALID_STATES.get(mapped.lower()) or LOWER_CASE_VALID_STATES.get(clean_str)
    if lower_map:
        return lower_map
        
    return None

def normalize_district(raw):
    if not raw:
        return 'Unknown'
    s = raw.replace('*', '')
    s = re.sub(r'[^\w\s-]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s.title() 

# Request Coalescing Map
_inflight_requests = {}

async def get_aggregate_insights(dataset: str, year: str = 'all'):
    cache_key = f"agg_v7:{dataset}:{year or 'all'}"
    
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
    local_path = os.path.join(os.getcwd(), 'public', 'datasets', local_subdir, file_name)
    
    process_path = ""
    is_temp = False
    
    if os.path.exists(local_path):
        logger.info(f"Using local file: {local_path}")
        process_path = local_path
    else:
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
        
        # 1. Normalize State vectorized
        # Convert state and pincode to strings for mapping
        df['state'] = df['state'].astype(str).str.lower().str.strip()
        df['pincode'] = df['pincode'].astype(str).str.split('.').str[0] 
        
        # Basic state normalization via map
        # Note: We still use the standard map for consistent naming
        state_map_lower = {k.lower(): v for k, v in STATE_STANDARD_MAP.items()}
        df['norm_state'] = df['state'].map(state_map_lower)
        
        # Pincode fallback for missing states
        missing_mask = df['norm_state'].isna()
        if missing_mask.any():
            df.loc[missing_mask, 'norm_state'] = df.loc[missing_mask, 'pincode'].map(PINCODE_MAP)
        
        # Drop rows where state is still unknown or not in valid list
        valid_set = set(VALID_STATES)
        df = df[df['norm_state'].isin(valid_set)]
        
        if df.empty:
            return result # Empty fallback

        # 2. Extract Month from Date
        df['date'] = df['date'].astype(str)
        # Handle various formats: yyyy-mm-dd or dd-mm-yyyy
        # Extract the middle part or first part depending on length
        def extract_month(d):
            parts = re.split(r'[-/]', d)
            if len(parts) == 3:
                return parts[1] # Usually month is middle in both standard formats
            return 'Unknown'
            
        df['month'] = df['date'].apply(extract_month)

        # 3. Calculate Counts based on dataset
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
        
        # 4. Global Aggregates
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
        
        # 5. By District (Per State)
        df['district'] = df['district'].astype(str).str.replace('*', '').str.strip().str.title()
        dist_agg = df.groupby(['norm_state', 'district'])['total'].sum().reset_index()
        for idx, row in dist_agg.iterrows():
            st, dst, val = row['norm_state'], row['district'], row['total']
            if st not in result['by_district']: result['by_district'][st] = {}
            result['by_district'][st][dst] = int(val)
            
        # 6. Detailed State Breakdown
        # This is more complex but can be done efficiently
        for label, col in age_groups.items():
            s_age = df.groupby('norm_state')[col].sum().to_dict()
            for st, val in s_age.items():
                if st not in result['state_breakdown']: result['state_breakdown'][st] = {"by_age_group": {}, "by_month": {}}
                result['state_breakdown'][st]['by_age_group'][label] = int(val)
                
        month_breakdown = df[df['month'] != 'Unknown'].groupby(['norm_state', 'month'])['total'].sum().reset_index()
        for idx, row in month_breakdown.iterrows():
            st, mon, val = row['norm_state'], row['month'], row['total']
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
