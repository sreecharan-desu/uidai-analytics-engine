
import pandas as pd
import io
import json
import re
import time
import os
from fastapi import HTTPException
from app.core.config import settings
from upstash_redis import Redis

# Constants
MASTER_CSV_URL = "https://github.com/sreecharan-desu/uidai-analytics-engine/releases/download/dataset-latest/aadhaar_powerbi_master.csv"
BIO_URL = 'https://github.com/sreecharan-desu/uidai-analytics-engine/releases/download/dataset-latest/biometric_full.csv'
DEMO_URL = 'https://github.com/sreecharan-desu/uidai-analytics-engine/releases/download/dataset-latest/demographic_full.csv'
ENROLL_URL = 'https://github.com/sreecharan-desu/uidai-analytics-engine/releases/download/dataset-latest/enrolment_full.csv'

# Maps (Notebook Compliance)
STATE_STANDARD_MAP = {
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
    'andaman and nicobar islands': 'Andaman and Nicobar Islands', 'andaman nicobar islands': 'Andaman and Nicobar Islands',
    'chandigarh': 'Chandigarh',
    'dadra and nagar haveli and daman and diu': 'Dadra and Nagar Haveli and Daman and Diu',
    'dadra nagar haveli': 'Dadra and Nagar Haveli and Daman and Diu',
    'daman and diu': 'Dadra and Nagar Haveli and Daman and Diu',
    'daman diu': 'Dadra and Nagar Haveli and Daman and Diu',
    'delhi': 'Delhi', 'new delhi': 'Delhi',
    'jammu and kashmir': 'Jammu and Kashmir', 'jammu kashmir': 'Jammu and Kashmir',
    'ladakh': 'Ladakh',
    'lakshadweep': 'Lakshadweep',
    'puducherry': 'Puducherry', 'pondicherry': 'Puducherry',
    'chhatisgarh': 'Chhattisgarh',
    'hyd': 'Telangana', 'hyderabad': 'Telangana', 'wb': 'West Bengal', 'up': 'Uttar Pradesh', 'mp': 'Madhya Pradesh'
}

DISTRICT_ALIAS_MAP = {
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

def _generate_fallback_data() -> pd.DataFrame:
    """Fallback: downloads 3 parts and merges them (Heavy)."""
    try:
        bio_cols = ['state', 'district', 'date', 'pincode', 'bio_age_5_17', 'bio_age_17_']
        demo_cols = ['state', 'district', 'date', 'pincode', 'demo_age_5_17', 'demo_age_17_']
        enroll_cols = ['state', 'district', 'date', 'pincode', 'age_0_5', 'age_5_17', 'age_18_greater']

        print("Fetching Biometric Data (Fallback)...")
        df_bio = pd.read_csv(BIO_URL, usecols=lambda c: c in bio_cols)
        df_bio['total_biometric_updates'] = df_bio.get('bio_age_5_17', 0).fillna(0) + df_bio.get('bio_age_17_', 0).fillna(0)
        
        print("Fetching Demographic Data (Fallback)...")
        df_demo = pd.read_csv(DEMO_URL, usecols=lambda c: c in demo_cols)
        df_demo['total_demographic_updates'] = df_demo.get('demo_age_5_17', 0).fillna(0) + df_demo.get('demo_age_17_', 0).fillna(0)
        
        print("Fetching Enrolment Data (Fallback)...")
        df_enroll = pd.read_csv(ENROLL_URL, usecols=lambda c: c in enroll_cols)
        df_enroll['total_enrolment'] = (df_enroll.get('age_0_5', 0).fillna(0) + 
                                        df_enroll.get('age_5_17', 0).fillna(0) + 
                                        df_enroll.get('age_18_greater', 0).fillna(0))
        
        master_df = pd.concat([df_bio, df_demo, df_enroll], ignore_index=True)
        
        # Metric columns filling (Notebook standard)
        metric_cols = [
            'bio_age_5_17', 'bio_age_17_', 
            'demo_age_5_17', 'demo_age_17_', 
            'age_0_5', 'age_5_17', 'age_18_greater', 
            'total_biometric_updates', 'total_enrolment'
        ]
        for col in metric_cols:
            if col in master_df.columns:
                master_df[col] = master_df[col].fillna(0)
        
        # Normalize
        master_df['state_norm'] = master_df['state'].apply(normalize_text)
        master_df['state_clean'] = master_df['state_norm'].map(STATE_STANDARD_MAP)
        master_df['state_clean'] = master_df['state_clean'].fillna(master_df['state'].str.title())
        
        master_df['district_norm'] = master_df['district'].astype(str).str.lower().str.strip().str.replace(r'\s+', ' ', regex=True)
        normalized_alias_map = {k.lower(): v for k, v in DISTRICT_ALIAS_MAP.items()}
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
        
        master_df['date'] = pd.to_datetime(master_df['date'], dayfirst=True, errors='coerce')
        master_df['month_year'] = master_df['date'].dt.to_period('M').astype(str)
        
        # Calculated Columns - Exact Notebook logic
        master_df['total_demographic_updates'] = master_df['demo_age_5_17'] + master_df['demo_age_17_']
        master_df['total_activity'] = (
            master_df.get('total_biometric_updates', 0) + 
            master_df.get('total_enrolment', 0) + 
            master_df.get('total_demographic_updates', 0)
        )
        
        master_df['biometric_update_ratio'] = (master_df['total_biometric_updates'] / master_df['total_activity']).fillna(0)
        master_df['demographic_update_ratio'] = (master_df['total_demographic_updates'] / master_df['total_activity']).fillna(0)

        return master_df
    except Exception as e:
        print(f"Fallback generation error: {e}")
        raise e

def get_master_partitions():
    """Returns a list of URLs for the yearly master partitions from GitHub."""
    import requests
    try:
        url = "https://api.github.com/repos/sreecharan-desu/uidai-analytics-engine/releases/tags/dataset-latest"
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200: return []
        
        assets = resp.json().get('assets', [])
        partition_urls = [
            asset['browser_download_url'] 
            for asset in assets 
            if asset['name'].startswith('master_') and asset['name'].endswith('.csv')
        ]
        return sorted(partition_urls)
    except Exception as e:
        print(f"Error fetching partitions: {e}")
        return []
