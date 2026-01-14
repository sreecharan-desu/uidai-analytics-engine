
import re
import pandas as pd
from app.utils.state_data import STATE_STANDARD_MAP, VALID_STATES, DISTRICT_ALIAS_MAP
import json
import os

# Load PINCODE_MAP
PINCODE_MAP = {}
try:
    path = os.path.join(os.getcwd(), 'app', 'core', 'pincodeMap.json')
    if os.path.exists(path):
        with open(path, 'r') as f:
            PINCODE_MAP = json.load(f)
except:
    pass

state_map_lower = {k.lower(): v for k, v in STATE_STANDARD_MAP.items()}

def normalize_text_basic(x):
    """Basic text normalization: lowercase, strip, remove special chars."""
    if not isinstance(x, str) and pd.isna(x): return None
    x = str(x).lower().strip()
    x = re.sub(r'[^a-z0-9 ]', ' ', x)
    x = re.sub(r'\s+', ' ', x).strip()
    return x

def normalize_district_name(val):
    """Normalize district names using standard regex and alias map."""
    if not isinstance(val, str) and pd.isna(val): return 'Unknown'
    x = str(val).lower().strip()
    x = x.replace('*', '') 
    x = re.sub(r'[^a-z0-9 \-\(\)\.]', ' ', x)
    x = re.sub(r'\s+', ' ', x).strip()
    x = DISTRICT_ALIAS_MAP.get(x, x)
    return x.title()

def get_normalized_state(row, state_col='state', pincode_col='pincode'):
    """
    Determines the normalized state name for a row.
    Uses STATE_STANDARD_MAP first, then falls back to PINCODE_MAP.
    """
    raw = normalize_text_basic(row.get(state_col))
    norm = state_map_lower.get(raw)
    
    if not norm and pincode_col in row and pd.notna(row[pincode_col]):
        pin = str(row[pincode_col]).split('.')[0]
        norm = PINCODE_MAP.get(pin)
        
    return norm

def clean_dataframe(df, dataset_name):
    """
    Applies Standard Cleaning Logic to a DataFrame.
    1. Normalizes State
    2. Filters Invalid States (Strict Mode for specific datasets)
    3. Normalizes District
    4. Extracts Year (if date present)
    """
    
    # 1. Identify Columns
    cols = df.columns
    state_col = next((c for c in ['state', 'State'] if c in cols), None)
    dist_col = next((c for c in ['district', 'District'] if c in cols), None)
    pin_col = next((c for c in ['pincode', 'Pincode'] if c in cols), None)
    date_col = next((c for c in ['date', 'Date'] if c in cols), None)

    if not state_col:
        print(f"Warning: No state column found for {dataset_name}")
        return df

    # 2. State Normalization
    # We use apply(axis=1) for row-wise fallback logic. 
    # For large datasets, vectorized approaches generally preferred but fallback to pincode requires row context or careful merge.
    # To keep it consistent across scripts, we'll implement a vectorized version if possible, or robust apply.
    
    # Vectorized Map
    df['__state_norm_raw'] = df[state_col].map(normalize_text_basic)
    df['norm_state'] = df['__state_norm_raw'].map(state_map_lower)
    
    # Vectorized Pincode Fallback
    if pin_col:
        mask = df['norm_state'].isna()
        if mask.any():
            df.loc[mask, 'norm_state'] = df.loc[mask, pin_col].astype(str).str.split('.').str[0].map(PINCODE_MAP)
            
    # 3. Strict Filtering
    # STRICT_DATASETS = ['enrolment', 'biometric', 'demographic']
    # We now enforce strict filtering for demographic as well, as we have improved the state map.
    if dataset_name in ['enrolment', 'biometric', 'demographic']:
        df = df[df['norm_state'].isin(VALID_STATES)].copy()
    else:
        # Fallback for unknown datasets
        df['norm_state'] = df['norm_state'].fillna(df[state_col])
    
    df[state_col] = df['norm_state']
    df.drop(columns=['__state_norm_raw', 'norm_state'], inplace=True, errors='ignore')

    # 4. District Normalization
    if dist_col:
        df[dist_col] = df[dist_col].apply(normalize_district_name)

    # 5. Year Extraction
    if date_col:
        def get_year(d):
            if pd.isna(d): return None
            parts = re.split(r'[-/]', str(d))
            if len(parts) == 3:
                if len(parts[0]) == 4: return parts[0]
                if len(parts[2]) == 4: return parts[2]
            return None
        df['year'] = df[date_col].apply(get_year)
        df = df[df['year'].notna()] # Drop rows where year parsing failed
    
    return df
