import requests
import os
import re

# --- Notebook-Grade Cleaning Logic Sources ---

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
    # UTs
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
    'chhatisgarh': 'Chhattisgarh'
}

DISTRICT_ALIAS_MAP = {
    "baleshwar": "Balasore",
    "dang": "The Dangs",
    "tamulpur district": "Tamulpur",
    "yadadri.": "Yadadri",
    "yadadri": "Yadadri",
    "medchal malkajgiri": "Medchal-Malkajgiri",
    "mahrajganj": "Maharajganj",
    "maharajganj": "Maharajganj",

    # 1. Karnataka
    "bangalore": "Bengaluru",
    "bangalore rural": "Bengaluru Rural",
    "bangalore urban": "Bengaluru Urban",
    "bengaluru south": "Bengaluru",
    "bengaluru urban": "Bengaluru",
    "bengaluru rural": "Bengaluru Rural",
    "gulbarga": "Kalaburagi",
    "belgaum": "Belagavi",
    "bellary": "Ballari",
    "bijapur": "Vijayapura",
    "chikmagalur": "Chikkamagaluru",
    "chikkamagaluru": "Chikkamagaluru",
    "chickmagalur": "Chikkamagaluru",
    "shimoga": "Shivamogga",
    "mysore": "Mysuru",
    "chamarajanagar": "Chamarajanagara",
    "chamrajnagar": "Chamarajanagara",
    "chamrajanagar": "Chamarajanagara",
    "mangalore": "Dakshina Kannada",
    "dakshina kannada": "Dakshina Kannada",
    "davanagere": "Davangere",
    "davangere": "Davangere",
    "hubli": "Dharwad",
    "hubballi": "Dharwad",
    "hasan": "Hassan",
    "ramanagar": "Ramanagara",
    
    # 2. Maharashtra
    "ahmednagar": "Ahilyanagar",
    "ahmed nagar": "Ahilyanagar",
    "aurangabad": "Chhatrapati Sambhajinagar",
    "osmanabad": "Dharashiv",
    "beed": "Bid",
    "buldhana": "Buldana",
    "gondia": "Gondiya",
    "raigarh(mh)": "Raigad",
    "raigarh": "Raigad",
    "bombay": "Mumbai",
    "mumbai suburban": "Mumbai Suburban",
    "mumbai": "Mumbai",
    "jalgaon": "Jalgaon",
    
    # 3. Punjab
    "ferozepur": "Firozpur",
    "s.a.s nagar": "S.A.S. Nagar",
    "mohali": "S.A.S. Nagar",
    "sas nagar mohali": "S.A.S. Nagar",
    "s.a.s nagar (mohali)": "S.A.S. Nagar",
    "s.a.s. nagar": "S.A.S. Nagar",
    "muktsar": "Sri Muktsar Sahib",
    
    # 4. West Bengal
    "burdwan": "Purba Bardhaman",
    "bardhaman": "Purba Bardhaman",
    "coochbehar": "Cooch Behar",
    "darjiling": "Darjeeling",
    "hooghly": "Hooghly",
    "howrah": "Howrah",
    "north 24 parganas": "North 24 Parganas",
    "north twenty four parganas": "North 24 Parganas",
    "south 24 parganas": "South 24 Parganas",
    "south twenty four parganas": "South 24 Parganas",
    "24 paraganas south": "South 24 Parganas",
    "puruliya": "Purulia",
    "malda": "Maldah",
    
    # 5. J&K / Ladakh
    "baramulla": "Baramula",
    "bandipora": "Bandipore",
    "budgam": "Badgam",
    "shupiyan": "Shopian",
    "punch": "Poonch",
    "leh": "Leh",
    "ladakh": "Leh",
    "rajauri": "Rajouri",
    
    # 6. Chhattisgarh
    "janjgir-champa": "Janjgir-Champa",
    "janjgir champa": "Janjgir-Champa",
    "kabeerdham": "Kabirdham",
    "koriya": "Korea",
    "mohla-manpur-ambagarh chouki": "Mohla-Manpur-Ambagarh Chowki",
    "mohla manpur ambagarh chouki": "Mohla-Manpur-Ambagarh Chowki",
    "mohalla-manpur-ambagarh chouki": "Mohla-Manpur-Ambagarh Chowki",
    "gaurela-pendra-marwahi": "Gaurella Pendra Marwahi",
    "gaurela pendra marwahi": "Gaurella Pendra Marwahi",
    "sarangarh-bilaigarh": "Sarangarh Bilaigarh",
    
    # 7. Andhra / Telangana
    "kadapa": "Y.S.R. Kadapa",
    "y.s.r. kadapa": "Y.S.R. Kadapa",
    "y s r kadapa": "Y.S.R. Kadapa",
    "ysr district": "Y.S.R. Kadapa",
    "mahbubnagar": "Mahabubnagar",
    "warangal urban": "Hanumakonda",
    "dr. b. r. ambedkar konaseema": "Dr. B.R. Ambedkar Konaseema",
    "dr b r ambedkar konaseema": "Dr. B.R. Ambedkar Konaseema",
    "n. t. r": "NTR",
    "n.t.r": "NTR",
    "sri potti sriramulu nellore": "Nellore",
    "yadadri": "Yadadri",
    "yadadri.": "Yadadri",
    "medchal malkajgiri": "Medchal-Malkajgiri",
    
    # 8. Tamil Nadu
    "kancheepuram": "Kanchipuram",
    "thiruvallur": "Tiruvallur",
    "thoothukudi": "Thoothukkudi",
    "tuticorin": "Thoothukkudi",
    "kanyakumari": "Kanniyakumari",
    "villupuram": "Viluppuram",
    "thiruvarur": "Tiruvarur",
    "tirupathur": "Tirupattur",
    
    # 9. UP / Bihar / Odisha / Others
    "allahabad": "Prayagraj",
    "faizabad": "Ayodhya",
    "lakhimpur kheri": "Kheri",
    "sant ravidas nagar": "Bhadohi",
    "sant ravidas nagar bhadohi": "Bhadohi",
    "bara banki": "Barabanki",
    "bulandshahar": "Bulandshahr",
    "baghpat": "Bagpat",
    "shravasti": "Shrawasti",
    "maharajganj": "Mahrajganj",
    
    "baleswar": "Balasore",
    "keonjhar": "Kendujhar",
    "nabarangapur": "Nabarangpur",
    "jagatsinghapur": "Jagatsinghpur",
    "anugul": "Angul",
    "baudh": "Boudh",
    "subarnapur": "Sonepur",
    "sonapur": "Sonepur",
    "jajapur": "Jajpur",
    "khorda": "Khordha",
    "sundargarh": "Sundergarh",
    
    "kaimur (bhabua)": "Kaimur",
    "kaimur bhabua": "Kaimur",
    "bhabua": "Kaimur",
    "purbi champaran": "East Champaran",
    "paschim champaran": "West Champaran",
    "jehanabad": "Jehanabad",
    "monghyr": "Munger",
    "sheikhpura": "Sheikpura",
    "samstipur": "Samastipur",
    "samastipur": "Samastipur",
    
    "ahmadabad": "Ahmedabad",
    "dohad": "Dahod",
    "mahesana": "Mehsana",
    "panchmahals": "Panchmahal",
    "banaskantha": "Banaskantha",
    "sabarkantha": "Sabarkantha",
    "surendra nagar": "Surendranagar",
    
    "gurgaon": "Gurugram",
    "mewat": "Nuh",
    "yamuna nagar": "Yamunanagar",
    
    "palamau": "Palamu",
    "pashchimi singhbhum": "West Singhbhum",
    "purbi singhbhum": "East Singhbhum",
    "saraikela-kharsawan": "Seraikela Kharsawan",
    "seraikela-kharsawan": "Seraikela Kharsawan",
    "hazaribag": "Hazaribagh",
    "kodarma": "Koderma",
    "pakaur": "Pakur",
    "sahebganj": "Sahibganj",
    "simdega": "Simdega",
    "lohardaga": "Lohardaga", # Verify
    
    "narsimhapur": "Narsinghpur",
    "hoshangabad": "Narmadapuram",
    "ashok nagar": "Ashoknagar",
    
    "kamrup metro": "Kamrup Metropolitan",
    "south salmara mankachar": "South Salmara-Mankachar",
    "ri-bhoi": "Ri Bhoi",
    "mamit": "Mammit",
    
    "chittaurgarh": "Chittorgarh",
    "jalor": "Jalore",
    "jhunjhunu": "Jhunjhunun",
    "didwana-kuchaman": "Didwana Kuchaman",
    "khairthal-tijara": "Khairthal Tijara",
    "kotputli-behror": "Kotputli Behror",
    
    "lahaul and spiti": "Lahul and Spiti",
    "shi yomi": "Shi Yomi",
    "shi-yomi": "Shi Yomi",
    "nicobar": "Nicobars",
    "kasaragod": "Kasargod"
}

def normalize_text(x):
    if x is None:
        return x
    x = str(x).lower().strip()
    x = re.sub(r'[^a-z0-9 ]', ' ', x)
    x = re.sub(r'\s+', ' ', x)
    return x

def get_master_partitions():
    """Returns a list of URLs for the yearly master partitions from GitHub."""
    try:
        url = "https://api.github.com/repos/sreecharan-desu/uidai-analytics-engine/releases/tags/dataset-latest"
        # Using a generic User-Agent to avoid blocks
        resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        
        if resp.status_code != 200:
            print(f"GitHub API Error: {resp.status_code}")
            return []
        
        assets = resp.json().get('assets', [])
        partition_urls = [
            asset['browser_download_url'] 
            for asset in assets 
            if asset['name'].startswith('master_') and asset['name'].endswith('.csv')
        ]
        
        # Sort to ensure order (e.g. 2024, 2025, 2026)
        return sorted(partition_urls)
    except Exception as e:
        print(f"Error fetching partitions: {e}")
        return []
