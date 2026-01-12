import os
import json
import csv
import re
import httpx
import tempfile
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

async def get_aggregate_insights(dataset: str, year: str = 'all'):
    cache_key = f"agg_v7:{dataset}:{year or 'all'}"
    
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
            
            async with httpx.AsyncClient(follow_redirects=True) as client:
                async with client.stream("GET", url) as response:
                    if response.status_code != 200:
                        raise ValueError(f"Failed to download dataset. Status: {response.status_code}")
                    
                    # Write chunks to temp file
                    # Note: synchronous write in async loop is blocking but acceptable for /tmp writing 
                    # compared to complex async csv parsing
                    async for chunk in response.aiter_bytes():
                        temp_file.write(chunk)
            temp_file.close()
            logger.info(f"Downloaded to {process_path}")
            
        except Exception as e:
            if is_temp and os.path.exists(process_path):
                os.remove(process_path)
            raise ValueError(f"Download failed: {str(e)}")

    logger.info(f"Processing aggregation for {file_name}...")
    
    result = {
        "total_updates": 0,
        "by_state": {},
        "by_district": {},
        "by_age_group": {},
        "by_month": {},
        "state_breakdown": {}
    }
    
    try:
        with open(process_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                state_raw = row.get('state')
                pincode = row.get('pincode')
                
                state = normalize_state(state_raw, pincode)
                if not state:
                    continue
                    
                district = normalize_district(row.get('district') or 'Unknown')
                
                date_str = row.get('date', '')
                month = 'Unknown'
                if date_str:
                    parts = re.split(r'[-/]', date_str)
                    if len(parts) == 3:
                        if len(parts[2]) == 4: # dd-mm-yyyy
                            month = parts[1]
                        elif len(parts[0]) == 4: # yyyy-mm-dd
                            month = parts[1]
                            
                count = 0
                age_counts = {}
                
                if dataset == 'biometric':
                    c1 = int(row.get('bio_age_5_17') or 0)
                    c2 = int(row.get('bio_age_17_') or 0)
                    count = c1 + c2
                    age_counts['5-17'] = c1
                    age_counts['18+'] = c2
                elif dataset == 'enrolment':
                    c1 = int(row.get('age_0_5') or 0)
                    c2 = int(row.get('age_5_17') or 0)
                    c3 = int(row.get('age_18_greater') or 0)
                    count = c1 + c2 + c3
                    age_counts['0-5'] = c1
                    age_counts['5-17'] = c2
                    age_counts['18+'] = c3
                elif dataset == 'demographic':
                    c1 = int(row.get('demo_age_5_17') or 0)
                    c2 = int(row.get('demo_age_17_') or 0)
                    count = c1 + c2
                    age_counts['5-17'] = c1
                    age_counts['18+'] = c2
                    
                if count == 0:
                    continue
                    
                result['total_updates'] += count
                
                # By State
                result['by_state'][state] = result['by_state'].get(state, 0) + count
                
                if state not in result['state_breakdown']:
                    result['state_breakdown'][state] = {"by_age_group": {}, "by_month": {}}
                
                # By District
                if state not in result['by_district']:
                    result['by_district'][state] = {}
                result['by_district'][state][district] = result['by_district'][state].get(district, 0) + count
                
                # By Age
                for g, c in age_counts.items():
                    result['by_age_group'][g] = result['by_age_group'].get(g, 0) + c
                    s_age = result['state_breakdown'][state]['by_age_group']
                    s_age[g] = s_age.get(g, 0) + c
                    
                # By Month
                if month != 'Unknown':
                    result['by_month'][month] = result['by_month'].get(month, 0) + count
                    s_mon = result['state_breakdown'][state]['by_month']
                    s_mon[month] = s_mon.get(month, 0) + count
        
        # Save to Cache
        await redis_client.set(cache_key, json.dumps(result), ex=86400)
        memory_cache[cache_key] = result
        
        # Cleanup Temp
        if is_temp and os.path.exists(process_path):
            os.remove(process_path)
            
        return result
        
    except Exception as e:
        logger.error(f"Error processing CSV: {e}")
        if is_temp and os.path.exists(process_path):
            os.remove(process_path)
        raise ValueError(f"Processing failed: {str(e)}")

async def prewarm_cache():
    logger.info("🔥 Pre-warming Analytics Cache...")
    datasets = ['enrolment', 'biometric', 'demographic']
    import asyncio
    try:
        await asyncio.gather(*[get_aggregate_insights(ds, '2025') for ds in datasets])
        logger.info("✅ Cache Pre-warmed Successfully!")
    except Exception as e:
        logger.error(f"❌ Cache Pre-warming Failed {e}")
