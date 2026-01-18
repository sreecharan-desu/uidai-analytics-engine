
import os
import requests
import pandas as pd
import json
import re
import sys
import numpy as np
from datetime import datetime
from dotenv import load_dotenv

# Add project root to path to import app modules if needed
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.config import settings
from app.utils.cleaning_utils import clean_dataframe

load_dotenv()

DATA_GOV_API_KEY = os.getenv("DATA_GOV_API_KEY")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

if not DATA_GOV_API_KEY:
    print("Error: DATA_GOV_API_KEY not found in environment.")
    sys.exit(1)

# === POWERBI INTEGRATION MAPS (Notebook Compliance) ===
NB_STATE_STANDARD_MAP = {
    'andhra pradesh': 'Andhra Pradesh', 'arunachal pradesh': 'Arunachal Pradesh', 'assam': 'Assam',
    'bihar': 'Bihar', 'chhattisgarh': 'Chhattisgarh', 'goa': 'Goa', 'gujarat': 'Gujarat',
    'haryana': 'Haryana', 'himachal pradesh': 'Himachal Pradesh', 'jharkhand': 'Jharkhand',
    'karnataka': 'Karnataka', 'kerala': 'Kerala', 'madhya pradesh': 'Madhya Pradesh',
    'maharashtra': 'Maharashtra', 'manipur': 'Manipur', 'meghalaya': 'Meghalaya',
    'mizoram': 'Mizoram', 'nagaland': 'Nagaland', 'odisha': 'Odisha', 'orissa': 'Odisha',
    'punjab': 'Punjab', 'rajasthan': 'Rajasthan', 'sikkim': 'Sikkim', 'tamil nadu': 'Tamil Nadu',
    'tamilnadu': 'Tamil Nadu', 'telangana': 'Telangana', 'tripura': 'Tripura',
    'uttar pradesh': 'Uttar Pradesh', 'uttarakhand': 'Uttarakhand', 'uttaranchal': 'Uttarakhand',
    'west bengal': 'West Bengal', 'westbengal': 'West Bengal', 'west bangal': 'West Bengal',
    'andaman and nicobar islands': 'Andaman and Nicobar Islands', 'chandigarh': 'Chandigarh',
    'dadra and nagar haveli and daman and diu': 'Dadra and Nagar Haveli and Daman and Diu',
    'dadra nagar haveli': 'Dadra and Nagar Haveli and Daman and Diu',
    'daman and diu': 'Dadra and Nagar Haveli and Daman and Diu',
    'delhi': 'Delhi', 'new delhi': 'Delhi', 'jammu and kashmir': 'Jammu and Kashmir',
    'ladakh': 'Ladakh', 'lakshadweep': 'Lakshadweep', 'puducherry': 'Puducherry'
}

NB_DISTRICT_ALIAS_MAP = {
    "baleshwar": "Balasore", "dang": "The Dangs", "tamulpur district": "Tamulpur",
    "yadadri.": "Yadadri", "yadadri": "Yadadri", "medchal malkajgiri": "Medchal-Malkajgiri",
    "mahrajganj": "Maharajganj", "maharajganj": "Maharajganj",
    "bangalore": "Bengaluru", "bangalore rural": "Bengaluru Rural", "bangalore urban": "Bengaluru Urban",
    "bengaluru south": "Bengaluru", "bengaluru urban": "Bengaluru", "bengaluru rural": "Bengaluru Rural",
    "gulbarga": "Kalaburagi", "belgaum": "Belagavi", "bellary": "Ballari", "bijapur": "Vijayapura",
    "chikmagalur": "Chikkamagaluru", "chikkamagaluru": "Chikkamagaluru", "chickmagalur": "Chikkamagaluru",
    "shimoga": "Shivamogga", "mysore": "Mysuru", "chamarajanagar": "Chamarajanagara",
    "chamrajnagar": "Chamarajanagara", "chamrajanagar": "Chamarajanagara", "mangalore": "Dakshina Kannada",
    "dakshina kannada": "Dakshina Kannada", "davanagere": "Davangere", "davangere": "Davangere",
    "hubli": "Dharwad", "hubballi": "Dharwad", "hasan": "Hassan", "ramanagar": "Ramanagara",
    "ahmednagar": "Ahilyanagar", "ahmed nagar": "Ahilyanagar", "aurangabad": "Chhatrapati Sambhajinagar",
    "osmanabad": "Dharashiv", "beed": "Bid", "buldhana": "Buldana", "gondia": "Gondiya",
    "raigarh(mh)": "Raigad", "raigarh": "Raigad", "bombay": "Mumbai", "mumbai suburban": "Mumbai Suburban",
    "mumbai": "Mumbai", "jalgaon": "Jalgaon", "ferozepur": "Firozpur", "s.a.s nagar": "S.A.S. Nagar",
    "mohali": "S.A.S. Nagar", "sas nagar mohali": "S.A.S. Nagar", "s.a.s nagar (mohali)": "S.A.S. Nagar",
    "s.a.s. nagar": "S.A.S. Nagar", "muktsar": "Sri Muktsar Sahib", "burdwan": "Purba Bardhaman",
    "bardhaman": "Purba Bardhaman", "coochbehar": "Cooch Behar", "darjiling": "Darjeeling",
    "hooghly": "Hooghly", "howrah": "Howrah", "north 24 parganas": "North 24 Parganas",
    "north twenty four parganas": "North 24 Parganas", "south 24 parganas": "South 24 Parganas",
    "south twenty four parganas": "South 24 Parganas", "24 paraganas south": "South 24 Parganas",
    "puruliya": "Purulia", "malda": "Maldah", "baramulla": "Baramula", "bandipora": "Bandipore",
    "budgam": "Badgam", "shupiyan": "Shopian", "punch": "Poonch", "leh": "Leh", "ladakh": "Leh",
    "rajauri": "Rajouri", "janjgir-champa": "Janjgir-Champa", "janjgir champa": "Janjgir-Champa",
    "kabeerdham": "Kabirdham", "koriya": "Korea", "mohla-manpur-ambagarh chouki": "Mohla-Manpur-Ambagarh Chowki",
    "mohla manpur ambagarh chouki": "Mohla-Manpur-Ambagarh Chowki", "mohalla-manpur-ambagarh chouki": "Mohla-Manpur-Ambagarh Chowki",
    "gaurela-pendra-marwahi": "Gaurella Pendra Marwahi", "gaurela pendra marwahi": "Gaurella Pendra Marwahi",
    "sarangarh-bilaigarh": "Sarangarh Bilaigarh", "kadapa": "Y.S.R. Kadapa", "y.s.r. kadapa": "Y.S.R. Kadapa",
    "y s r kadapa": "Y.S.R. Kadapa", "ysr district": "Y.S.R. Kadapa", "mahbubnagar": "Mahabubnagar",
    "warangal urban": "Hanumakonda", "dr. b. r. ambedkar konaseema": "Dr. B.R. Ambedkar Konaseema",
    "dr b r ambedkar konaseema": "Dr. B.R. Ambedkar Konaseema", "n. t. r": "NTR", "n.t.r": "NTR",
    "sri potti sriramulu nellore": "Nellore", "yadadri": "Yadadri", "yadadri.": "Yadadri",
    "medchal malkajgiri": "Medchal-Malkajgiri", "kancheepuram": "Kanchipuram", "thiruvallur": "Tiruvallur",
    "thoothukudi": "Thoothukkudi", "tuticorin": "Thoothukkudi", "kanyakumari": "Kanniyakumari",
    "villupuram": "Viluppuram", "thiruvarur": "Tiruvarur", "tirupathur": "Tirupattur", "allahabad": "Prayagraj",
    "faizabad": "Ayodhya", "lakhimpur kheri": "Kheri", "sant ravidas nagar": "Bhadohi",
    "sant ravidas nagar bhadohi": "Bhadohi", "bara banki": "Barabanki", "bulandshahar": "Bulandshahr",
    "baghpat": "Bagpat", "shravasti": "Shrawasti", "maharajganj": "Mahrajganj", "baleswar": "Balasore",
    "keonjhar": "Kendujhar", "nabarangapur": "Nabarangpur", "jagatsinghapur": "Jagatsinghpur", "anugul": "Angul",
    "baudh": "Boudh", "subarnapur": "Sonepur", "sonapur": "Sonepur", "jajapur": "Jajpur", "khorda": "Khordha",
    "sundargarh": "Sundergarh", "kaimur (bhabua)": "Kaimur", "kaimur bhabua": "Kaimur", "bhabua": "Kaimur",
    "purbi champaran": "East Champaran", "paschim champaran": "West Champaran", "jehanabad": "Jehanabad",
    "monghyr": "Munger", "sheikhpura": "Sheikpura", "samstipur": "Samastipur", "samastipur": "Samastipur",
    "ahmadabad": "Ahmedabad", "dohad": "Dahod", "mahesana": "Mehsana", "panchmahals": "Panchmahal",
    "banaskantha": "Banaskantha", "sabarkantha": "Sabarkantha", "surendra nagar": "Surendranagar",
    "gurgaon": "Gurugram", "mewat": "Nuh", "yamuna nagar": "Yamunanagar", "palamau": "Palamu",
    "pashchimi singhbhum": "West Singhbhum", "purbi singhbhum": "East Singhbhum",
    "saraikela-kharsawan": "Seraikela Kharsawan", "seraikela-kharsawan": "Seraikela Kharsawan",
    "hazaribag": "Hazaribagh", "kodarma": "Koderma", "pakaur": "Pakur", "sahebganj": "Sahibganj",
    "simdega": "Simdega", "lohardaga": "Lohardaga", "narsimhapur": "Narsinghpur", "hoshangabad": "Narmadapuram",
    "ashok nagar": "Ashoknagar", "kamrup metro": "Kamrup Metropolitan", "south salmara mankachar": "South Salmara-Mankachar",
    "ri-bhoi": "Ri Bhoi", "mamit": "Mammit", "chittaurgarh": "Chittorgarh", "jalor": "Jalore",
    "jhunjhunu": "Jhunjhunun", "didwana-kuchaman": "Didwana Kuchaman", "khairthal-tijara": "Khairthal Tijara",
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
    """Download existing full CSV from GitHub only if not present locally."""
    local_path = os.path.join(os.getcwd(), 'public', 'datasets', f"{dataset_name}_full.csv")
    
    # Check if exists
    if os.path.exists(local_path):
        print(f"✅ Local {dataset_name} found. Skipping download.")
        return local_path
        
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    repo = 'sreecharan-desu/uidai-analytics-engine'
    url = f"https://github.com/{repo}/releases/download/dataset-latest/{dataset_name}_full.csv"
    
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
    if not file_path or not os.path.exists(file_path): return 0
    try:
        with open(file_path, 'rb') as f:
            lines = sum(1 for _ in f)
        return max(0, lines - 1)
    except: return 0

def fetch_incremental_records(resource_id, dataset_name, start_offset=0):
    base_url = f"https://api.data.gov.in/resource/{resource_id}"
    params = {
        "api-key": DATA_GOV_API_KEY,
        "format": "json",
        "limit": 10000,
        "offset": start_offset if start_offset < 4800000 else 0
    }
    
    # Deep paging logic
    is_deep_paging = (dataset_name == "biometric" and start_offset >= 4800000)
    if is_deep_paging:
        params["sort[date]"] = "desc"
        params["offset"] = 0 
        params["limit"] = 10000

    all_new_records = []
    
    try:
        resp = requests.get(base_url, params=params, timeout=30)
        if resp.status_code != 200: return []
        
        data = resp.json()
        if data.get("status") != "ok": return []

        current_total = int(data.get("total", 0))
        total_matching = 20000 if is_deep_paging else (current_total - start_offset)
        
        records = data.get("records", [])
        all_new_records.extend(records)
        offset = params["offset"] + len(records)
        
        while len(all_new_records) < total_matching:
            params["offset"] = offset
            resp = requests.get(base_url, params=params, timeout=30)
            if resp.status_code != 200: break
            
            data = resp.json()
            records = data.get("records", [])
            if not records: break
            
            all_new_records.extend(records)
            offset += len(records)
            
            if offset >= 5000000 and not is_deep_paging: break

    except Exception as e:
        print(f"Incremental fetch failed: {e}")
        
    print(f"\nFetched {len(all_new_records)} records.")
    return all_new_records

def process_and_merge(dataset_name, local_base_path, new_records):
    # Load
    if local_base_path and os.path.exists(local_base_path):
        df_base = pd.read_csv(local_base_path)
    else:
        df_base = pd.DataFrame()

    df_new = pd.DataFrame()
    if new_records:
        df_new = pd.DataFrame(new_records)
        df_new = clean_dataframe(df_new, dataset_name)
    
    if df_new.empty and not new_records:
        if df_base.empty: return
        df_final = df_base
    else:
        if not df_base.empty and not df_new.empty:
            df_combined = pd.concat([df_base, df_new], ignore_index=True)
            subset_cols = [c for c in df_combined.columns if c not in ['source_dataset', 'year']]
            df_final = df_combined.drop_duplicates(subset=subset_cols, keep='last')
        else:
            df_final = pd.concat([df_base, df_new], ignore_index=True)

    if df_final.empty: return

    # Save ONLY full CSV
    output_dir = os.path.join(os.getcwd(), 'public', 'datasets')
    os.makedirs(output_dir, exist_ok=True)
    full_path = os.path.join(output_dir, f"{dataset_name}_full.csv")
    
    if 'year' in df_final.columns:
        df_final.drop(columns=['year']).to_csv(full_path, index=False)
    else:
        df_final.to_csv(full_path, index=False)
    
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
        
        # Exact Metric Columns from Notebook
        metric_cols = [
            'bio_age_5_17', 'bio_age_17_', 
            'demo_age_5_17', 'demo_age_17_', 
            'age_0_5', 'age_5_17', 'age_18_greater', 
            'total_biometric_updates', 'total_enrolment'
        ]
        for col in metric_cols:
            if col in master_df.columns:
                master_df[col] = master_df[col].fillna(0)

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

        # Structural Audit (Majority Vote)
        district_state_counts = master_df.groupby(['district', 'state']).size().reset_index(name='count')
        authoritative_map = district_state_counts.sort_values('count', ascending=False).drop_duplicates('district')[['district', 'state']]
        authoritative_dict = dict(zip(authoritative_map['district'], authoritative_map['state']))
        
        manual_overrides = {
            'Leh': 'Ladakh', 'Kargil': 'Ladakh',
            'Mahabubnagar': 'Telangana', 'Rangareddy': 'Telangana', 'Khammam': 'Telangana'
        }
        authoritative_dict.update(manual_overrides)
        master_df['state'] = master_df['district'].map(authoritative_dict).fillna(master_df['state'])
        
        # Date Handling - Enforce dayfirst=True for DD-MM-YYYY source format
        master_df['date'] = pd.to_datetime(master_df['date'], dayfirst=True, errors='coerce')
        master_df.dropna(subset=['date'], inplace=True) 
        
        master_df['month_year'] = master_df['date'].dt.to_period('M').astype(str)
        
        # Ensure year for partitioning if needed
        if 'year' not in master_df.columns:
            master_df['year'] = master_df['date'].dt.year

        # --- AGGREGATION for Power BI ---
        # Group by Key Dimensions to squash them into single rows per Location-Date.
        agg_key = ['state', 'district', 'date', 'pincode', 'month_year', 'year']
        
        agg_map = {col: 'sum' for col in metric_cols if col in master_df.columns}
        
        print("Aggregating Master Dataset for PowerBI...")
        aggregated_df = master_df.groupby(agg_key, as_index=False).agg(agg_map)
        
        # Recalculate Ratios on Aggregated Data
        aggregated_df['total_demographic_updates'] = aggregated_df.get('demo_age_5_17', 0) + aggregated_df.get('demo_age_17_', 0)
        
        aggregated_df.fillna(0, inplace=True)
        
        aggregated_df['total_activity'] = (
            aggregated_df['total_biometric_updates'] + 
            aggregated_df['total_enrolment'] + 
            aggregated_df['total_demographic_updates']
        )
        
        aggregated_df['biometric_update_ratio'] = np.where(
            aggregated_df['total_activity'] > 0,
            aggregated_df['total_biometric_updates'] / aggregated_df['total_activity'],
            0
        )
        
        aggregated_df['demographic_update_ratio'] = np.where(
            aggregated_df['total_activity'] > 0,
            aggregated_df['total_demographic_updates'] / aggregated_df['total_activity'],
            0
        )

        # Save local full backup (Aggregated version)
        # This is what gets uploaded to GitHub as 'aadhaar_powerbi_master.csv' (or master_dataset_final)
        out_path_csv = os.path.join(dataset_dir, 'master_dataset_final.csv')
        aggregated_df.to_csv(out_path_csv, index=False)
        
        print(f"Master Dataset Generated. Total rows (Aggregated): {len(aggregated_df)}")

    except Exception as e:
        print(f"Error generating PowerBI Master: {e}")

def push_to_github(updated_datasets=None):
    """
    Selectively pushes updated files to GitHub Releases.
    updated_datasets: List of dataset names that were actually changed.
    """
    print("\n=== Selective Push to GitHub ===")
    dataset_dir = os.path.join(os.getcwd(), 'public', 'datasets')
    tag = "dataset-latest"
    
    # 1. Upload the 'full' bases only (Biometric, Demographic, Enrolment)
    if updated_datasets:
        for ds in updated_datasets:
            fpath = os.path.join(dataset_dir, f"{ds}_full.csv")
            if os.path.exists(fpath):
                print(f"Uploading updated base: {ds}_full.csv")
                os.system(f"gh release upload {tag} {fpath} --clobber")

    # 2. Upload Master Dataset (master_dataset_final.csv)
    # The generated file is in public/datasets/master_dataset_final.csv
    
    master_generated = os.path.join(dataset_dir, 'master_dataset_final.csv')
    
    if os.path.exists(master_generated):
        print(f"Uploading Generated Master: {master_generated}")
        os.system(f"gh release upload {tag} {master_generated} --clobber")
    else:
        # Fallback to notebook artifact if generated one is missing (unlikely)
        master_notebook = os.path.join(os.getcwd(), 'public', 'notebooks', 'master_dataset_final.csv')
        if os.path.exists(master_notebook):
             print(f"Uploading Master (Notebook Artifact): {master_notebook}")
             os.system(f"gh release upload {tag} {master_notebook} --clobber")

    # 3. Git metadata push
    print("Committing metadata...")
    os.system("git add .")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    os.system(f'git commit -m "Auto-sync (Datasets): {timestamp}" --allow-empty')
    os.system("git push origin main")
    print("Push complete.")

def main():
    datasets = settings.RESOURCES
    updated_datasets = []
    
    for name, rid in datasets.items():
        local_file = download_existing_from_github(name)
        count = get_local_record_count(local_file)
        new_records = fetch_incremental_records(rid, name, start_offset=count)
        if new_records:
            process_and_merge(name, local_file, new_records)
            updated_datasets.append(name) 
    
    # Always generate/refresh the master dataset logic (it reads from the updated CSVs)
    generate_powerbi_master()
    
    # Push only modified parts + Master
    push_to_github(updated_datasets)
        
    print("\nIncremental Sync & Selective Push Complete!")

if __name__ == "__main__":
    main()
