
import pandas as pd
import os
import sys
import re
import json

# Add project root
sys.path.append(os.getcwd())

from app.utils.state_data import STATE_STANDARD_MAP, VALID_STATES, DISTRICT_ALIAS_MAP

# Load PINCODE_MAP manually
PINCODE_MAP = {}
try:
    with open('app/config/pincodeMap.json', 'r') as f:
        PINCODE_MAP = json.load(f)
except:
    pass

def normalize_text(x):
    if not isinstance(x, str): return None
    x = x.lower().strip()
    x = re.sub(r'[^a-z0-9 ]', ' ', x)
    x = re.sub(r'\s+', ' ', x).strip()
    return x

def normalize_district(x):
    if not isinstance(x, str): return 'Unknown'
    x = x.lower().strip()
    x = x.replace('*', '') 
    x = re.sub(r'[^a-z0-9 \-\(\)\.]', ' ', x)
    x = re.sub(r'\s+', ' ', x).strip()
    return x

def clean_biometric():
    file_path = "public/datasets/biometric_full.csv"
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    print("Loading biometric_full.csv...")
    df = pd.read_csv(file_path, low_memory=False)
    print(f"Original Count: {len(df)}")

    # 1. State Cleaning
    state_map_lower = {k.lower(): v for k, v in STATE_STANDARD_MAP.items()}
    
    # Ensure columns exist
    state_col = 'state'
    if state_col not in df.columns:
        if 'State' in df.columns: state_col = 'State'

    # Clean State
    df['state_str'] = df[state_col].astype(str).apply(normalize_text)
    df['norm_state'] = df['state_str'].map(state_map_lower)
    
    # Fallback to Pincode
    if 'pincode' in df.columns:
        pincode_series = df['pincode'].astype(str).str.split('.').str[0]
        df.loc[df['norm_state'].isna(), 'norm_state'] = pincode_series.map(PINCODE_MAP)

    # Filter Valid
    df_clean = df[df['norm_state'].isin(VALID_STATES)].copy()
    print(f"After Strict State Filtering: {len(df_clean)}")
    
    # Set cleaned state
    df_clean[state_col] = df_clean['norm_state']
    df_clean.drop(columns=['state_str', 'norm_state'], inplace=True)

    # 2. District Cleaning
    dist_col = 'district'
    if dist_col not in df_clean.columns:
        if 'District' in df_clean.columns: dist_col = 'District'
    
    if dist_col in df_clean.columns:
        df_clean['dist_norm'] = df_clean[dist_col].astype(str).apply(normalize_district)
        df_clean['dist_norm'] = df_clean['dist_norm'].replace(DISTRICT_ALIAS_MAP)
        df_clean[dist_col] = df_clean['dist_norm'].str.title()
        df_clean.drop(columns=['dist_norm'], inplace=True)

    # 3. Year Extraction (for splitting)
    if 'date' in df_clean.columns:
        def get_year(d):
            if not isinstance(d, str): return 'Unknown'
            parts = re.split(r'[-/]', d)
            if len(parts) == 3:
                # Assuming dd-mm-yyyy or yyyy-mm-dd
                if len(parts[0]) == 4: return parts[0]
                if len(parts[2]) == 4: return parts[2]
            return 'Unknown'
        df_clean['year'] = df_clean['date'].astype(str).apply(get_year)

    # Save Full
    print("Saving cleaned biometric_full.csv...")
    df_clean.drop(columns=['year']).to_csv(file_path, index=False)

    # Save Splits
    split_dir = "public/datasets/split_data"
    os.makedirs(split_dir, exist_ok=True)
    
    for year, group in df_clean.groupby('year'):
        if year == 'Unknown': continue
        out_path = os.path.join(split_dir, f"biometric_{year}.csv")
        print(f"Saving {out_path} ({len(group)} rows)")
        group.drop(columns=['year']).to_csv(out_path, index=False)

if __name__ == "__main__":
    clean_biometric()
