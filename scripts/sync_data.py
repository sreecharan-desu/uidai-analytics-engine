
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

# === POWERBI INTEGRATION MAPS (Notebook Compliance) ===
NB_STATE_STANDARD_MAP = {
    'andhra pradesh': 'Andhra Pradesh', 'telangana': 'Telangana', 'hyd': 'Telangana', 'hyderabad': 'Telangana',
    'jammu and kashmir': 'Jammu and Kashmir', 'j & k': 'Jammu and Kashmir', 'ladakh': 'Ladakh',
    'arunachal pradesh': 'Arunachal Pradesh', 'assam': 'Assam', 'manipur': 'Manipur', 'meghalaya': 'Meghalaya',
    'mizoram': 'Mizoram', 'nagaland': 'Nagaland', 'tripura': 'Tripura', 'sikkim': 'Sikkim',
    'delhi': 'Delhi', 'new delhi': 'Delhi', 'nct of delhi': 'Delhi', 'chandigarh': 'Chandigarh',
    'puducherry': 'Puducherry', 'pondicherry': 'Puducherry', 'lakshadweep': 'Lakshadweep',
    'andaman and nicobar islands': 'Andaman and Nicobar Islands', 'a & n islands': 'Andaman and Nicobar Islands',
    'dadra and nagar haveli': 'Dadra and Nagar Haveli and Daman and Diu',
    'daman and diu': 'Dadra and Nagar Haveli and Daman and Diu',
    'dadra and nagar haveli and daman and diu': 'Dadra and Nagar Haveli and Daman and Diu',
    'dnh & dd': 'Dadra and Nagar Haveli and Daman and Diu',
    'odisha': 'Odisha', 'orissa': 'Odisha', 'west bengal': 'West Bengal', 'wb': 'West Bengal',
    'uttarakhand': 'Uttarakhand', 'uttaranchal': 'Uttarakhand', 'chhattisgarh': 'Chhattisgarh',
    'chattisgarh': 'Chhattisgarh', 'punjab': 'Punjab', 'haryana': 'Haryana', 'himachal pradesh': 'Himachal Pradesh',
    'uttar pradesh': 'Uttar Pradesh', 'up': 'Uttar Pradesh', 'bihar': 'Bihar', 'jharkhand': 'Jharkhand',
    'rajasthan': 'Rajasthan', 'gujarat': 'Gujarat', 'maharashtra': 'Maharashtra', 'goa': 'Goa',
    'karnataka': 'Karnataka', 'kerala': 'Kerala', 'tamil nadu': 'Tamil Nadu', 'tn': 'Tamil Nadu',
    'madhya pradesh': 'Madhya Pradesh', 'mp': 'Madhya Pradesh'
}

NB_DISTRICT_ALIAS_MAP = {
    "belagavi": "Belagavi", "belgaum": "Belagavi", "bengaluru": "Bengaluru", "bangalore": "Bengaluru",
    "bengaluru urban": "Bengaluru Urban", "bangalore urban": "Bengaluru Urban",
    "bengaluru rural": "Bengaluru Rural", "bangalore rural": "Bengaluru Rural",
    "kalaburagi": "Kalaburagi", "gulbarga": "Kalaburagi", "mysuru": "Mysuru", "mysore": "Mysuru",
    "shivamogga": "Shivamogga", "shimoga": "Shivamogga", "vijayapura": "Vijayapura", "bijapur": "Vijayapura",
    "ballari": "Ballari", "bellary": "Ballari", "chikkamagaluru": "Chikkamagaluru",
    "chikmagalur": "Chikkamagaluru", "chamarajanagara": "Chamarajanagara", "chamrajanagar": "Chamarajanagara",
    "mangalore": "Dakshina Kannada", "dakshina kannada": "Dakshina Kannada", "davanagere": "Davangere",
    "davangere": "Davangere", "hubli": "Dharwad", "hubballi": "Dharwad", "hasan": "Hassan",
    "ramanagar": "Ramanagara", "ahmednagar": "Ahilyanagar", "ahmed nagar": "Ahilyanagar",
    "aurangabad": "Chhatrapati Sambhajinagar", "osmanabad": "Dharashiv", "beed": "Bid", "buldhana": "Buldana",
    "gondia": "Gondiya", "raigarh(mh)": "Raigad", "raigarh": "Raigad", "bombay": "Mumbai",
    "mumbai suburban": "Mumbai Suburban", "mumbai": "Mumbai", "jalgaon": "Jalgaon",
    "ferozepur": "Firozpur", "s.a.s nagar": "S.A.S. Nagar", "mohali": "S.A.S. Nagar",
    "sas nagar mohali": "S.A.S. Nagar", "s.a.s nagar (mohali)": "S.A.S. Nagar",
    "s.a.s. nagar": "S.A.S. Nagar", "muktsar": "Sri Muktsar Sahib",
    "burdwan": "Purba Bardhaman", "bardhaman": "Purba Bardhaman", "coochbehar": "Cooch Behar",
    "darjiling": "Darjeeling", "hooghly": "Hooghly", "howrah": "Howrah",
    "north 24 parganas": "North 24 Parganas", "north twenty four parganas": "North 24 Parganas",
    "south 24 parganas": "South 24 Parganas", "south twenty four parganas": "South 24 Parganas",
    "24 paraganas south": "South 24 Parganas", "puruliya": "Purulia", "malda": "Maldah",
    "baramulla": "Baramula", "bandipora": "Bandipore", "budgam": "Badgam", "shupiyan": "Shopian",
    "punch": "Poonch", "leh": "Leh", "ladakh": "Leh", "rajauri": "Rajouri",
    "janjgir-champa": "Janjgir-Champa", "janjgir champa": "Janjgir-Champa", "kabeerdham": "Kabirdham",
    "koriya": "Korea", "mohla-manpur-ambagarh chouki": "Mohla-Manpur-Ambagarh Chowki",
    "mohla manpur ambagarh chouki": "Mohla-Manpur-Ambagarh Chowki",
    "mohalla-manpur-ambagarh chouki": "Mohla-Manpur-Ambagarh Chowki",
    "gaurela-pendra-marwahi": "Gaurella Pendra Marwahi", "gaurela pendra marwahi": "Gaurella Pendra Marwahi",
    "sarangarh-bilaigarh": "Sarangarh Bilaigarh", "kadapa": "Y.S.R. Kadapa",
    "y.s.r. kadapa": "Y.S.R. Kadapa", "y s r kadapa": "Y.S.R. Kadapa", "ysr district": "Y.S.R. Kadapa",
    "mahbubnagar": "Mahabubnagar", "warangal urban": "Hanumakonda",
    "dr. b. r. ambedkar konaseema": "Dr. B.R. Ambedkar Konaseema",
    "dr b r ambedkar konaseema": "Dr. B.R. Ambedkar Konaseema", "n. t. r": "NTR", "n.t.r": "NTR",
    "sri potti sriramulu nellore": "Nellore", "yadadri": "Yadadri", "yadadri.": "Yadadri",
    "medchal malkajgiri": "Medchal-Malkajgiri", "kancheepuram": "Kanchipuram", "thiruvallur": "Tiruvallur",
    "thoothukudi": "Thoothukkudi", "tuticorin": "Thoothukkudi", "kanyakumari": "Kanniyakumari",
    "villupuram": "Viluppuram", "thiruvarur": "Tiruvarur", "tirupathur": "Tirupattur",
    "allahabad": "Prayagraj", "faizabad": "Ayodhya", "lakhimpur kheri": "Kheri",
    "sant ravidas nagar": "Bhadohi", "sant ravidas nagar bhadohi": "Bhadohi", "bara banki": "Barabanki",
    "bulandshahar": "Bulandshahr", "baghpat": "Bagpat", "shravasti": "Shrawasti",
    "maharajganj": "Mahrajganj", "baleswar": "Balasore", "keonjhar": "Kendujhar",
    "nabarangapur": "Nabarangpur", "jagatsinghapur": "Jagatsinghpur", "anugul": "Angul", "baudh": "Boudh",
    "subarnapur": "Sonepur", "sonapur": "Sonepur", "jajapur": "Jajpur", "khorda": "Khordha",
    "sundargarh": "Sundergarh", "kaimur (bhabua)": "Kaimur", "kaimur bhabua": "Kaimur", "bhabua": "Kaimur",
    "purbi champaran": "East Champaran", "paschim champaran": "West Champaran", "jehanabad": "Jehanabad",
    "monghyr": "Munger", "sheikhpura": "Sheikpura", "samstipur": "Samastipur", "samastipur": "Samastipur",
    "ahmadabad": "Ahmedabad", "dohad": "Dahod", "mahesana": "Mehsana", "panchmahals": "Panchmahal",
    "banaskantha": "Banaskantha", "sabarkantha": "Sabarkantha", "surendra nagar": "Surendranagar",
    "gurgaon": "Gurugram", "mewat": "Nuh", "yamuna nagar": "Yamunanagar", "palamau": "Palamu",
    "pashchimi singhbhum": "West Singhbhum", "purbi singhbhum": "East Singhbhum",
    "saraikela-kharsawan": "Seraikela Kharsawan", "seraikela-kharsawan": "Seraikela Kharsawan",
    "hazaribag": "Hazaribagh", "kodarma": "Koderma", "pakaur": "Pakur", "sahebganj": "Sahibganj",
    "simdega": "Simdega", "lohardaga": "Lohardaga", "narsimhapur": "Narsinghpur",
    "hoshangabad": "Narmadapuram", "ashok nagar": "Ashoknagar", "kamrup metro": "Kamrup Metropolitan",
    "south salmara mankachar": "South Salmara-Mankachar", "ri-bhoi": "Ri Bhoi", "mamit": "Mammit",
    "chittaurgarh": "Chittorgarh", "jalor": "Jalore", "jhunjhunu": "Jhunjhunun",
    "didwana-kuchaman": "Didwana Kuchaman", "khairthal-tijara": "Khairthal Tijara",
    "kotputli-behror": "Kotputli Behror", "lahaul and spiti": "Lahul and Spiti", "shi yomi": "Shi Yomi",
    "shi-yomi": "Shi Yomi", "nicobar": "Nicobars", "kasaragod": "Kasargod"
}

def normalize_text(x):
    if pd.isna(x):
        return x
    x = str(x).lower().strip()
    x = re.sub(r'[^a-z0-9 ]', ' ', x)
    x = re.sub(r'\s+', ' ', x)
    return x

def download_existing_from_github(dataset_name):
    """Download existing full CSV from GitHub to find the starting point."""
    repo = 'sreecharan-desu/uidai-analytics-engine'
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
        with open(file_path, 'rb') as f:
            lines = sum(1 for _ in f)
        return max(0, lines - 1)
    except:
        return 0

def fetch_incremental_records(resource_id, dataset_name, start_offset=0):
    base_url = f"https://api.data.gov.in/resource/{resource_id}"
    params = {
        "api-key": DATA_GOV_API_KEY,
        "format": "json",
        "limit": 10000,
        "offset": start_offset if start_offset < 4800000 else 0
    }
    
    # Implementing "Search After" logic for large datasets like biometric
    # If offset exceeds 4.8M, we switch to date-filtering to bypass max_result_window
    is_deep_paging = (dataset_name == "biometric" and start_offset >= 4800000)
    
    if is_deep_paging:
        # Cursor lookup: Find the latest date in our local dataset.
        local_path = os.path.join(os.getcwd(), 'public', 'datasets', f"{dataset_name}_full.csv")
        if os.path.exists(local_path):
            try:
                # Optimized tail lookup for large files
                df_last = pd.read_csv(local_path).tail(2000)
                if not df_last.empty and 'date' in df_last.columns:
                    latest_date = df_last['date'].max()
                    # Query for records from the latest date onwards (Lucene Range Query)
                    params["query"] = f'date:["{latest_date}" TO *]'
                    params["sort"] = "date asc"
                    params["offset"] = 0 
                    print(f"Applying search_after strategy for {dataset_name} (Cursor: {latest_date})")
            except Exception as e:
                print(f"Warning: search_after cursor lookup failed: {e}")

    all_new_records = []
    
    try:
        resp = requests.get(base_url, params=params, timeout=30)
        if resp.status_code != 200:
            print(f"Error checking status for {dataset_name}: {resp.status_code}")
            return []
        
        data = resp.json()
        if data.get("status") != "ok":
            error_msg = str(data.get('message'))
            print(f"API returned error for {dataset_name}: {error_msg}")
            
            # Auto-recovery if offset error happens unexpectedly
            if "Result window is too large" in error_msg and not is_deep_paging:
                 print("Attempting automatic deep-paging recovery...")
                 return fetch_incremental_records(resource_id, dataset_name, 5000000)
            return []

        current_total = int(data.get("total", 0))
        
        if not is_deep_paging and start_offset >= current_total:
            print(f"Dataset {dataset_name} is already up to date ({start_offset}/{current_total}).")
            return []
            
        print(f"Syncing {dataset_name} ({resource_id})...")
        if is_deep_paging:
            print(f"Deep-paging mode: Cursor '{params.get('query')}' (Results in window: {current_total})")
        else:
            print(f"Found {current_total - start_offset} new records (Total: {current_total}).")
        
        records = data.get("records", [])
        all_new_records.extend(records)
        
        # Determine how many records we've actually fetched vs how many match the current query
        offset = params["offset"] + len(records)
        total_matching = int(data.get("total", 0))
        
        while len(all_new_records) < total_matching:
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
            print(f"Progress: {len(all_new_records)}/{total_matching} fetched...", end="\r")
            
            # Safe boundary for max_result_window
            if offset >= 5000000 and not is_deep_paging:
                 print("\nWarning: Safety window limit reached. Stopping batch to prevent 500 error.")
                 break

    except Exception as e:
        print(f"Incremental fetch failed: {e}")
        
    print(f"\nFetched {len(all_new_records)} records.")
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
        df_new = clean_dataframe(df_new, dataset_name)
    
    if df_new.empty and not new_records:
        if df_base.empty:
            print(f"No data to process for {dataset_name}.")
            return
        df_final = df_base
    else:
        # Safe merge: de-duplicate against base if we fetched using date overlapping
        if not df_base.empty and not df_new.empty:
            df_combined = pd.concat([df_base, df_new], ignore_index=True)
            # Use all identifying columns for de-duplication
            # Exclude derived columns if any
            subset_cols = [c for c in df_combined.columns if c not in ['source_dataset', 'year']]
            df_final = df_combined.drop_duplicates(subset=subset_cols, keep='last')
        else:
            df_final = pd.concat([df_base, df_new], ignore_index=True)

    
    if df_final.empty:
        return

    # Helper to check for year column
    if 'year' not in df_final.columns and 'date' in df_final.columns:
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

def generate_powerbi_master():
    """
    Generates the master integrated CSV specific for PowerBI by reading the local files
    that were just synced/updated.
    """
    print("\n=== Generating PowerBI Master Dataset ===")
    
    dataset_dir = os.path.join(os.getcwd(), 'public', 'datasets')
    bio_path = os.path.join(dataset_dir, 'biometric_full.csv')
    demo_path = os.path.join(dataset_dir, 'demographic_full.csv')
    enroll_path = os.path.join(dataset_dir, 'enrolment_full.csv')
    
    if not (os.path.exists(bio_path) and os.path.exists(demo_path) and os.path.exists(enroll_path)):
        print("Required full datasets are missing in public/datasets. Skipping Master Generation.")
        return

    try:
        # Load columns strictly needed
        bio_cols = ['state', 'district', 'date', 'pincode', 'bio_age_5_17', 'bio_age_17_']
        demo_cols = ['state', 'district', 'date', 'pincode', 'demo_age_5_17', 'demo_age_17_']
        enroll_cols = ['state', 'district', 'date', 'pincode', 'age_0_5', 'age_5_17', 'age_18_greater']

        print("Reading datasets...")
        df_bio = pd.read_csv(bio_path, usecols=lambda c: c in bio_cols)
        df_demo = pd.read_csv(demo_path, usecols=lambda c: c in demo_cols)
        df_enroll = pd.read_csv(enroll_path, usecols=lambda c: c in enroll_cols)

        # Pre-calc totals
        df_bio['total_biometric_updates'] = df_bio.get('bio_age_5_17', 0).fillna(0) + df_bio.get('bio_age_17_', 0).fillna(0)
        df_demo['total_demographic_updates'] = df_demo.get('demo_age_5_17', 0).fillna(0) + df_demo.get('demo_age_17_', 0).fillna(0)
        df_enroll['total_enrolment'] = (df_enroll.get('age_0_5', 0).fillna(0) + 
                                        df_enroll.get('age_5_17', 0).fillna(0) + 
                                        df_enroll.get('age_18_greater', 0).fillna(0))

        # Cleanup & Tag
        df_bio_clean = df_bio.copy()
        df_bio_clean['source_dataset'] = 'Biometric Updates'
        
        df_demo_clean = df_demo.copy()
        df_demo_clean['source_dataset'] = 'Demographic Updates'
        
        df_enroll_clean = df_enroll.copy()
        df_enroll_clean['source_dataset'] = 'New Enrolment'
        
        # Merge
        master_df = pd.concat([df_bio_clean, df_demo_clean, df_enroll_clean], ignore_index=True)
        
        # Normalization
        master_df['state_norm'] = master_df['state'].apply(normalize_text)
        master_df['state_clean'] = master_df['state_norm'].map(NB_STATE_STANDARD_MAP)
        master_df['state_clean'] = master_df['state_clean'].fillna(master_df['state'].str.title())
        
        master_df['district_norm'] = master_df['district'].astype(str).str.lower().str.strip().str.replace(r'\s+', ' ', regex=True)
        normalized_alias_map = {k.lower(): v for k, v in NB_DISTRICT_ALIAS_MAP.items()}
        master_df['district_clean'] = master_df['district_norm'].replace(normalized_alias_map).str.title()
        
        master_df['state'] = master_df['state_clean']
        master_df['district'] = master_df['district_clean']
        master_df.drop(columns=['state_norm', 'state_clean', 'district_norm', 'district_clean'], inplace=True, errors='ignore')

        # Structural Audit
        district_state_counts = master_df.groupby(['district', 'state']).size().reset_index(name='count')
        authoritative_map = district_state_counts.sort_values('count', ascending=False).drop_duplicates('district')[['district', 'state']]
        authoritative_dict = dict(zip(authoritative_map['district'], authoritative_map['state']))
        
        manual_overrides = {
            'Leh': 'Ladakh', 'Kargil': 'Ladakh',
            'Mahabubnagar': 'Telangana', 'Rangareddy': 'Telangana', 'Khammam': 'Telangana'
        }
        authoritative_dict.update(manual_overrides)
        master_df['state'] = master_df['district'].map(authoritative_dict).fillna(master_df['state'])
        
        # Date Handling
        master_df['date'] = pd.to_datetime(master_df['date'], errors='coerce')
        master_df['month_year'] = master_df['date'].dt.to_period('M').astype(str)
        
        # Fill NaNs
        master_df['total_enrolment'] = master_df['total_enrolment'].fillna(0)
        master_df['total_biometric_updates'] = master_df['total_biometric_updates'].fillna(0)
        master_df['total_demographic_updates'] = master_df['total_demographic_updates'].fillna(0)
        
        master_df['total_activity'] = (master_df['total_enrolment'] + 
                                       master_df['total_biometric_updates'] + 
                                       master_df['total_demographic_updates'])
        
        # Ratios
        master_df['biometric_update_ratio'] = master_df['total_biometric_updates'] / master_df['total_activity']
        master_df['biometric_update_ratio'] = master_df['biometric_update_ratio'].fillna(0)
        
        master_df['demographic_update_ratio'] = master_df['total_demographic_updates'] / master_df['total_activity']
        master_df['demographic_update_ratio'] = master_df['demographic_update_ratio'].fillna(0)
        
        # Save Master CSV
        out_path_csv = os.path.join(dataset_dir, 'aadhaar_powerbi_master.csv')
        master_df.to_csv(out_path_csv, index=False)
        print(f"Generated Aadhaar PowerBI Master CSV: {out_path_csv}")

        # Save Master JSON (for API optimization)
        out_path_json = os.path.join(dataset_dir, 'aadhaar_powerbi_master.json')
        master_df.to_json(out_path_json, orient='records', date_format='iso')
        print(f"Generated Aadhaar PowerBI Master JSON: {out_path_json}")
        
        print(f"Master Dataset Generation Complete. Rows: {len(master_df)}")

    except Exception as e:
        print(f"Error generating PowerBI Master: {e}")

def main():
    datasets = settings.RESOURCES
    
    for name, rid in datasets.items():
        local_file = download_existing_from_github(name)
        count = get_local_record_count(local_file)
        new_records = fetch_incremental_records(rid, name, start_offset=count)
        process_and_merge(name, local_file, new_records)
    
    # Generate Integration Master after all are synced
    generate_powerbi_master()
        
    print("\nIncremental Sync Complete!")

if __name__ == "__main__":
    main()
