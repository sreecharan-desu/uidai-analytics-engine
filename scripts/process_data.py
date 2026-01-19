
import pandas as pd
import numpy as np
import re
import os

# ==========================================
# CONSTANTS & MAPS
# ==========================================

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
    'chhatisgarh': 'Chhattisgarh',
    'greater kailash 2': 'Delhi',
    'pune city': 'Maharashtra',
    'puthur': 'Kerala'
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
    "lohardaga": "Lohardaga",
    
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

VALID_STATES = {
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh",
    "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand",
    "Karnataka", "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur",
    "Meghalaya", "Mizoram", "Nagaland", "Odisha", "Punjab",
    "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana", "Tripura",
    "Uttar Pradesh", "Uttarakhand", "West Bengal",
    
    # Union Territories
    "Andaman and Nicobar Islands", "Chandigarh",
    "Dadra and Nagar Haveli and Daman and Diu",
    "Delhi", "Jammu and Kashmir", "Ladakh",
    "Lakshadweep", "Puducherry"
}

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def normalize_text(x):
    """Normalize text by lowercasing and removing special characters."""
    if pd.isna(x):
        return x
    x = str(x).lower().strip()
    x = re.sub(r'[^a-z0-9 ]', ' ', x)
    x = re.sub(r'\s+', ' ', x)
    return x


# Valid Districts Whitelist (Extracted from Ideal Dataset)
VALID_DISTRICTS = {
    "Adilabad", "Agar Malwa", "Agra", "Ahilyanagar", "Ahmedabad", "Aizawl", "Ajmer", "Akola", "Alappuzha", "Aligarh",
    "Alipurduar", "Alirajpur", "Alluri Sitharama Raju", "Almora", "Alwar", "Ambala", "Ambedkar Nagar", "Amethi",
    "Amravati", "Amreli", "Amritsar", "Amroha", "Anakapalli", "Anand", "Ananthapuramu", "Anantnag", "Andamans",
    "Angul", "Anjaw", "Annamayya", "Anugal", "Anuppur", "Araria", "Ariyalur", "Arvalli", "Arwal", "Ashoknagar",
    "Auraiya", "Ayodhya", "Azamgarh", "Badgam", "Bagalkot", "Bageshwar", "Bagpat", "Bahraich", "Bajali", "Baksa",
    "Balaghat", "Balangir", "Balasore", "Balianta", "Ballari", "Ballia", "Balod", "Baloda Bazar", "Balotra",
    "Balrampur", "Banaskantha", "Banda", "Bandipore", "Bandipur", "Banka", "Bankura", "Banswara", "Bapatla",
    "Barabanki", "Baramula", "Baran", "Bardez", "Bareilly", "Bargarh", "Barmer", "Barnala", "Barpeta", "Barwani",
    "Bastar", "Basti", "Bathinda", "Beawar", "Begusarai", "Belagavi", "Bemetara", "Bengaluru", "Bengaluru Rural",
    "Betul", "Bhadohi", "Bhadradri Kothagudem", "Bhadrak", "Bhagalpur", "Bhandara", "Bharatpur", "Bharuch", "Bhavnagar",
    "Bhilwara", "Bhind", "Bhiwani", "Bhojpur", "Bhopal", "Bicholim", "Bid", "Bidar", "Bijnor", "Bikaner", "Bilaspur",
    "Birbhum", "Bishnupur", "Biswanath", "Bokaro", "Bongaigaon", "Botad", "Boudh", "Budaun", "Bulandshahr", "Buldana",
    "Bundi", "Burhanpur", "Buxar", "Cachar", "Central Delhi", "Chamarajanagara", "Chamba", "Chamoli", "Champawat",
    "Champhai", "Chandauli", "Chandel", "Chandigarh", "Chandrapur", "Changlang", "Charaideo", "Charkhi Dadri",
    "Chatra", "Chengalpattu", "Chennai", "Chhatarpur", "Chhatrapati Sambhajinagar", "Chhindwara", "Chhotaudepur",
    "Chikkaballapur", "Chikkamagaluru", "Chirang", "Chitradurga", "Chitrakoot", "Chittoor", "Chittorgarh",
    "Chumukedima", "Churachandpur", "Churu", "Coimbatore", "Cooch Behar", "Cuddalore", "Cuttack",
    "Dadra And Nagar Haveli", "Dadra Nagar Haveli", "Dahod", "Dakshin Bastar Dantewada", "Dakshin Dinajpur",
    "Dakshina Kannada", "Daman", "Damoh", "Dantewada", "Darbhanga", "Darjeeling", "Darrang", "Datia", "Dausa",
    "Davangere", "Debagarh", "Deeg", "Dehradun", "Deoghar", "Deoria", "Devbhumi Dwarka", "Dewas", "Dhalai", "Dhamtari",
    "Dhanbad", "Dhar", "Dharashiv", "Dharmapuri", "Dharwad", "Dhaulpur", "Dhemaji", "Dhenkanal", "Dholpur", "Dhubri",
    "Dhule", "Dibang Valley", "Dibrugarh", "Didwana Kuchaman", "Dima Hasao", "Dimapur", "Dindigul", "Dindori", "Diu",
    "Doda", "Domjur", "Dr. B.R. Ambedkar Konaseema", "Dumka", "Dungarpur", "Durg", "East Champaran", "East Delhi",
    "East Garo Hills", "East Godavari", "East Jaintia Hills", "East Kameng", "East Khasi Hills", "East Nimar",
    "East Siang", "East Sikkim", "East Singhbhum", "Eastern West Khasi Hills", "Eluru", "Ernakulam", "Erode", "Etah",
    "Etawah", "Faridabad", "Faridkot", "Farrukhabad", "Fatehabad", "Fatehgarh Sahib", "Fatehpur", "Fazilka",
    "Firozabad", "Firozpur", "Gadag", "Gadchiroli", "Gajapati", "Ganderbal", "Gandhinagar", "Ganganagar", "Ganjam",
    "Garhwa", "Garhwal", "Gariyaband", "Gaurella Pendra Marwahi", "Gautam Buddha Nagar", "Gaya", "Ghaziabad",
    "Ghazipur", "Gir Somnath", "Giridih", "Goalpara", "Godda", "Golaghat", "Gomati", "Gonda", "Gondiya", "Gopalganj",
    "Gorakhpur", "Gumla", "Guna", "Guntur", "Gurdaspur", "Gurugram", "Gwalior", "Hailakandi", "Hamirpur", "Hanumakonda",
    "Hanumangarh", "Hapur", "Harda", "Hardoi", "Haridwar", "Hassan", "Hathin", "Hathras", "Haveri", "Hazaribagh",
    "Hingoli", "Hisar", "Hnahthial", "Hojai", "Hooghly", "Hoshiarpur", "Howrah", "Hyderabad", "Idukki", "Imphal East",
    "Imphal West", "Indore", "Jabalpur", "Jagatsinghpur", "Jagitial", "Jaintia Hills", "Jaipur", "Jaisalmer", "Jajpur",
    "Jalandhar", "Jalaun", "Jalgaon", "Jalna", "Jalore", "Jalpaiguri", "Jammu", "Jamnagar", "Jamtara", "Jamui",
    "Jangaon", "Jangoan", "Janjgir-Champa", "Jashpur", "Jaunpur", "Jayashankar Bhupalpally", "Jehanabad", "Jhabua",
    "Jhajjar", "Jhalawar", "Jhansi", "Jhargram", "Jharsuguda", "Jhunjhunun", "Jind", "Jiribam", "Jodhpur",
    "Jogulamba Gadwal", "Jorhat", "Junagadh", "Jyotiba Phule Nagar", "Kabirdham", "Kachchh", "Kaimur", "Kaithal",
    "Kakching", "Kakinada", "Kalaburagi", "Kalahandi", "Kalimpong", "Kallakurichi", "Kamareddy", "Kamle", "Kamrup",
    "Kamrup Metropolitan", "Kanchipuram", "Kandhamal", "Kangpokpi", "Kangra", "Kanker", "Kannauj", "Kanniyakumari",
    "Kannur", "Kanpur Dehat", "Kanpur Nagar", "Kapurthala", "Karaikal", "Karauli", "Karbi Anglong", "Kargil",
    "Karimganj", "Karimnagar", "Karnal", "Karur", "Kasargod", "Kasganj", "Kathua", "Katihar", "Katni", "Kaushambi",
    "Kawardha", "Kendrapara", "Kendujhar", "Khagaria", "Khairagarh Chhuikhadan Gandai", "Khairthal Tijara", "Khammam",
    "Khandwa", "Khargone", "Khawzawl", "Kheda", "Kheri", "Khordha", "Khowai", "Khunti", "Kinnaur", "Kiphire",
    "Kishanganj", "Kishtwar", "Koch Bihar", "Kodagu", "Koderma", "Kohima", "Kokrajhar", "Kolar", "Kolasib", "Kolhapur",
    "Kolkata", "Kollam", "Komaram Bheem", "Kondagaon", "Koppal", "Koraput", "Korba", "Korea", "Kota", "Kotputli Behror",
    "Kottayam", "Kozhikode", "Kra Daadi", "Krishna", "Krishnagiri", "Kulgam", "Kullu", "Kupwara", "Kurnool",
    "Kurukshetra", "Kurung Kumey", "Kushinagar", "Lahul And Spiti", "Lakhimpur", "Lakhisarai", "Lakshadweep",
    "Lalitpur", "Latehar", "Latur", "Lawngtlai", "Leh", "Leparada", "Lohardaga", "Lohit", "Longding", "Longleng",
    "Lower Dibang Valley", "Lower Siang", "Lower Subansiri", "Lucknow", "Ludhiana", "Lunglei", "Madhepura", "Madhubani",
    "Madurai", "Mahabubabad", "Mahabubnagar", "Maharajganj", "Mahasamund", "Mahendragarh", "Mahisagar", "Mahoba",
    "Mahrajganj", "Maihar", "Mainpuri", "Majuli", "Malappuram", "Maldah", "Malerkotla", "Malkangiri", "Mammit",
    "Mancherial", "Mandi", "Mandla", "Mandsaur", "Mandya", "Manendragarh Chirmiri Bharatpur", "Mangan", "Mansa",
    "Marigaon", "Mathura", "Mau", "Mauganj", "Mayiladuthurai", "Mayurbhanj", "Medak", "Medchal-Malkajgiri", "Meerut",
    "Mehsana", "Meluri", "Mirzapur", "Moga", "Mohalla-Manpur-Ambagarh Chowki", "Mohla-Manpur-Ambagarh Chowki",
    "Mokokchung", "Mon", "Moradabad", "Morbi", "Morena", "Mulugu", "Mumbai", "Mumbai City", "Mumbai Suburban",
    "Mungeli", "Munger", "Murshidabad", "Muzaffarnagar", "Muzaffarpur", "Mysuru", "Nabarangpur", "Nadia", "Nagaon",
    "Nagapattinam", "Nagarkurnool", "Nagaur", "Nagpur", "Nainital", "Najafgarh", "Nalanda", "Nalbari", "Nalgonda",
    "Namakkal", "Namchi", "Namsai", "Nanded", "Nandurbar", "Nandyal", "Narayanpet", "Narayanpur", "Narmada",
    "Narmadapuram", "Narsinghpur", "Nashik", "Navsari", "Nawada", "Nawanshahr", "Nayagarh", "Neemuch", "Nellore",
    "New Delhi", "Nicobars", "Nirmal", "Niuland", "Niwari", "Nizamabad", "Noklak", "North 24 Parganas",
    "North And Middle Andaman", "North Cachar Hills", "North Delhi", "North Dinajpur", "North East Delhi",
    "North Garo Hills", "North Goa", "North Sikkim", "North Tripura", "North West Delhi", "Ntr", "Nuapada", "Nuh",
    "Pakke Kessang", "Pakur", "Palakkad", "Palamu", "Palghar", "Pali", "Palnadu", "Palwal", "Panchkula", "Panchmahal",
    "Pandhurna", "Panipat", "Panna", "Papum Pare", "Parbhani", "Parvathipuram Manyam", "Paschim Bardhaman",
    "Paschim Medinipur", "Pashchim Champaran", "Patan", "Pathanamthitta", "Pathankot", "Patiala", "Patna",
    "Pauri Garhwal", "Peddapalli", "Perambalur", "Peren", "Phalodi", "Phek", "Pherzawl", "Pilibhit", "Pithoragarh",
    "Pondicherry", "Poonch", "Porbandar", "Prakasam", "Pratapgarh", "Prayagraj", "Puducherry", "Pudukkottai", "Pulwama",
    "Pune", "Purba Bardhaman", "Purba Champaran", "Purba Medinipur", "Puri", "Purnia", "Purulia", "Rae Bareli",
    "Raichur", "Raigad", "Raipur", "Raisen", "Rajanna Sircilla", "Rajgarh", "Rajkot", "Rajnandgaon", "Rajouri",
    "Rajsamand", "Ramanagara", "Ramanathapuram", "Ramban", "Ramgarh", "Rampur", "Ranchi", "Rangareddy", "Ranipet",
    "Ratlam", "Ratnagiri", "Rayagada", "Reasi", "Rewa", "Rewari", "Ri Bhoi", "Rohtak", "Rohtas", "Rudraprayag",
    "Rupnagar", "S.A.S. Nagar", "Sabarkantha", "Sagar", "Saharanpur", "Saharsa", "Sahibganj", "Saiha", "Saitual",
    "Sakti", "Salem", "Salumbar", "Samastipur", "Samba", "Sambalpur", "Sambhal", "Sangareddy", "Sangli", "Sangrur",
    "Sant Kabir Nagar", "Saran", "Sarangarh Bilaigarh", "Satara", "Satna", "Sawai Madhopur", "Sehore", "Senapati",
    "Seoni", "Sepahijala", "Seraikela Kharsawan", "Serchhip", "Shahdara", "Shahdol", "Shaheed Bhagat Singh Nagar",
    "Shahjahanpur", "Shajapur", "Shamator", "Shamli", "Sheikpura", "Sheohar", "Sheopur", "Shi Yomi", "Shimla",
    "Shivamogga", "Shivpuri", "Shopian", "Shrawasti", "Siang", "Sibsagar", "Siddharthnagar", "Siddipet", "Sidhi",
    "Sikar", "Simdega", "Sindhudurg", "Singrauli", "Sirmaur", "Sirohi", "Sirsa", "Sitamarhi", "Sitapur", "Sivaganga",
    "Sivasagar", "Siwan", "Solan", "Solapur", "Sonbhadra", "Sonepur", "Sonipat", "Sonitpur", "South 24 Parganas",
    "South Andaman", "South Delhi", "South Dinajpur", "South Dumdum", "South East Delhi", "South Garo Hills",
    "South Goa", "South Salmara-Mankachar", "South Sikkim", "South Tripura", "South West Delhi", "South West Garo Hills",
    "South West Khasi Hills", "Sri Muktsar Sahib", "Sri Sathya Sai", "Sribhumi", "Srikakulam", "Srinagar", "Sukma",
    "Sultanpur", "Sundergarh", "Supaul", "Surajpur", "Surat", "Surendranagar", "Surguja", "Suryapet", "Tamenglong",
    "Tamulpur", "Tapi", "Tarn Taran", "Tawang", "Tehri Garhwal", "Tenkasi", "Thane", "Thanjavur", "The Dangs",
    "The Nilgiris", "Theni", "Thiruvananthapuram", "Thoothukkudi", "Thoubal", "Thrissur", "Tikamgarh", "Tinsukia",
    "Tirap", "Tiruchirappalli", "Tirunelveli", "Tirupati", "Tirupattur", "Tiruppur", "Tiruvallur", "Tiruvannamalai",
    "Tiruvarur", "Tiswadi", "Tonk", "Tseminyu", "Tuensang", "Tumakuru", "Udaipur", "Udalguri", "Udham Singh Nagar",
    "Udhampur", "Udupi", "Ujjain", "Ukhrul", "Umaria", "Una", "Unakoti", "Unknown", "Unnao", "Upper Siang",
    "Upper Subansiri", "Uttar Bastar Kanker", "Uttar Dinajpur", "Uttara Kannada", "Uttarkashi", "Vadodara", "Vaishali",
    "Valsad", "Varanasi", "Vellore", "Vidisha", "Vijayanagara", "Vijayapura", "Vikarabad", "Viluppuram", "Virudhunagar",
    "Visakhapatnam", "Vizianagaram", "Wanaparthy", "Warangal", "Warangal Rural", "Wardha", "Washim", "Wayanad",
    "West Champaran", "West Delhi", "West Garo Hills", "West Godavari", "West Jaintia Hills", "West Kameng",
    "West Karbi Anglong", "West Khasi Hills", "West Nimar", "West Siang", "West Sikkim", "West Singhbhum", "West Tripura",
    "Wokha", "Y.S.R. Kadapa", "Yadadri", "Yadadri Bhuvanagiri", "Yadgir",
    "Yamunanagar", "Yanam", "Yavatmal", "Zunheboto"
}

def basic_clean(df):
    """Initial basic cleaning of state and district columns."""
    # Drop rows with invalid states like '100000' or garbage
    # Optimization: Don't drop '100000' yet. Let Pincode Recovery try to fix it.
    # if 'state' in df.columns:
    #     df = df[~df['state'].astype(str).str.contains('100000', na=False)]
    
    # Title regularization
    for col in ['state', 'district']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()

    # Filter/Normalize Districts based on Whitelist
    # MOVED: Filtering should happen AFTER normalization, not here.
    # if 'district' in df.columns:
    #     valid_mask = df['district'].isin(VALID_DISTRICTS)
    #     df.loc[~valid_mask, 'district'] = 'Unknown'

    return df

# ==========================================
# DATASET PROCESSORS
# ==========================================

def process_biometric(file_path):
    print(f"Processing Biometric Data from {file_path}...")
    df = pd.read_csv(file_path)
    df = basic_clean(df)
    
    # Ensure date parsing
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # Ensure required columns exist
    required_cols = ['bio_age_5_17', 'bio_age_17_']
    for col in required_cols:
        if col not in df.columns:
            df[col] = 0
            
    # Add metadata
    df['source_dataset'] = 'Biometric'
    df['total_biometric_updates'] = df['bio_age_5_17'] + df['bio_age_17_']
    
    return df

def process_enrollment(file_path):
    print(f"Processing Enrollment Data from {file_path}...")
    df = pd.read_csv(file_path)
    df = basic_clean(df)
    
    # Ensure date parsing
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # Ensure required columns exist
    required_cols = ['age_0_5', 'age_5_17', 'age_18_greater']
    for col in required_cols:
        if col not in df.columns:
            df[col] = 0
            
    # Add metadata
    df['source_dataset'] = 'Enrollment'
    df['total_enrolment'] = df['age_0_5'] + df['age_5_17'] + df['age_18_greater']
    
    return df

def process_demographic(file_path):
    print(f"Processing Demographic Data from {file_path}...")
    df = pd.read_csv(file_path)
    df = basic_clean(df)
    
    # Ensure date parsing (Demographic often has dd-mm-yyyy)
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y', errors='coerce')
    # Use standard format if above fails or mixed
    if df['date'].isna().sum() > len(df) * 0.5:
         df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Ensure required columns exist
    required_cols = ['demo_age_5_17', 'demo_age_17_']
    for col in required_cols:
        if col not in df.columns:
            df[col] = 0
            
    # Add metadata
    df['source_dataset'] = 'Demographic'
    df['total_demographic_updates'] = df['demo_age_5_17'] + df['demo_age_17_']
    
    return df

# ==========================================
# MAIN EXECUTION
# ==========================================

def integrate_datasets():
    # Define file paths
    # Define file paths
    base_dir = "public/datasets"
    bio_path = os.path.join(base_dir, "biometric.csv")
    enroll_path = os.path.join(base_dir, "enrollment.csv")
    demo_path = os.path.join(base_dir, "demographic.csv")
    
    # 1. Load and clean individual datasets
    df_bio = process_biometric(bio_path)
    df_enroll = process_enrollment(enroll_path)
    df_demo = process_demographic(demo_path)
    
    # 2. Prepare for Merge
    # Drop auxiliary columns to avoid conflict
    df_bio_clean = df_bio.drop(columns=['state_original', 'month'], errors='ignore')
    df_enroll_clean = df_enroll.drop(columns=['state_original', 'month'], errors='ignore')
    df_demo_clean = df_demo.drop(columns=['state_needs_correction', 'district_raw', 'month'], errors='ignore')
    
    # 3. Concatenate into Master DataFrame
    print("Merging datasets...")
    master_df = pd.concat([df_bio_clean, df_demo_clean, df_enroll_clean], ignore_index=True)
    
    # Fill NaNs for numerical analysis
    metric_cols = [
        'bio_age_5_17', 'bio_age_17_', 
        'demo_age_5_17', 'demo_age_17_', 
        'age_0_5', 'age_5_17', 'age_18_greater', 
        'total_biometric_updates', 'total_enrolment', 'total_demographic_updates'
    ]
    for col in metric_cols:
        if col in master_df.columns:
            master_df[col] = master_df[col].fillna(0)
            
    # Calculate Total Activity (Row-wise sum of available columns)
    master_df['total_activity'] = (
        master_df['total_biometric_updates'] + 
        master_df['total_enrolment'] + 
        master_df['total_demographic_updates']
    )
    
    return master_df

def apply_strict_normalization(master_df):
    print("Applying Strict Name Normalization...")
    
    # 1. State Normalization
    master_df['state_norm'] = master_df['state'].apply(normalize_text)
    master_df['state_clean'] = master_df['state_norm'].map(STATE_STANDARD_MAP)
    master_df['state_clean'].fillna(master_df['state'].str.title(), inplace=True)
    
    # 2. District Normalization
    # Standard Lower/Strip
    master_df['district_norm'] = master_df['district'].astype(str).str.lower().str.strip().str.replace(r'\s+', ' ', regex=True)
    
    # Map Replacements
    normalized_alias_map = {k.lower(): v for k, v in DISTRICT_ALIAS_MAP.items()}
    master_df['district_clean'] = master_df['district_norm'].replace(normalized_alias_map).str.title()
    
    # Update Standard Columns
    master_df['state'] = master_df['state_clean']
    master_df['district'] = master_df['district_clean']
    
    # Cleanup intermediate columns
    master_df.drop(columns=['state_norm', 'state_clean', 'district_norm', 'district_clean'], inplace=True, errors='ignore')
    
    # 3. Structural Audit (Majority Vote for District-State Consistency)
    # This ensures that if 'Aurangabad' appears mostly in Maharashtra, stray 'Aurangabad' entries in Bihar (if data error) get corrected or identified.
    # Note: Valid duplicate district names across states exist (e.g. Pratapgarh), but this map is a safety heuristic.
    # The original report script used this, so we retain it for consistency.
    
    district_state_counts = master_df.groupby(['district', 'state']).size().reset_index(name='count')
    authoritative_map = district_state_counts.sort_values('count', ascending=False).drop_duplicates('district')[['district', 'state']]
    authoritative_dict = dict(zip(authoritative_map['district'], authoritative_map['state']))
    
    # Explicit Overrides from Report
    manual_overrides = {
        'Leh': 'Ladakh', 'Kargil': 'Ladakh',
        'Mahabubnagar': 'Telangana', 'Rangareddy': 'Telangana', 'Khammam': 'Telangana'
    }
    authoritative_dict.update(manual_overrides)
    
    # Apply standard state mapping based on district
    # Note: This is aggressive. It assumes a district name matches strictly to ONE state.
    # Use with caution. The original script did key-value mapping filling NaNs.
    # Here we will only fill if state is missing or explicitly invalid, to avoid re-mapping valid duplicates like Bilaspur (HP/CG).
    # However, the original script line was:
    # master_df['state'] = master_df['district'].map(authoritative_dict).fillna(master_df['state'])
    # This DOES overwrite existing state if district is in dict. Given the user asked to "integrate all logic", I will use it but add a safety check for known duplicates if I knew them.
    # For now, I will trust the user's logic from the Report script.
    
    master_df['state'] = master_df['district'].map(authoritative_dict).fillna(master_df['state'])
    
    # ---------------------------------------------------------
    # 3.4 Valid List Enforcement (Moved from basic_clean)
    # ---------------------------------------------------------
    # Now that we have aliased "Baleshwar" -> "Balasore", we check validity.
    # Any district NOT in VALID_DISTRICTS becomes "Unknown"
    # This ensures we don't kill aliases early.
    
    # But wait, VALID_DISTRICTS contains the Target names.
    # So if we have "Balasore" now, it is valid.
    # If we have "Garbage", it becomes Unknown.
    
    valid_dist_mask = master_df['district'].isin(VALID_DISTRICTS)
    master_df.loc[~valid_dist_mask, 'district'] = 'Unknown'
    
    # ---------------------------------------------------------
    # 3.5 Pincode-Based Recovery (Crusial for recovering ~1.6M rows)
    # ---------------------------------------------------------
    print("Running Pincode Recovery for Missing/Invalid Locations...")
    
    # Identify "Trusted" rows for learning maps
    # Trusted = Valid State AND (Valid District OR (District is known valid alias))
    # Since we already aliased districts, we just check our maps.
    
    # Build Maps from currently valid data
    # (We use the state_clean and district_clean columns implicitly via the current state/dist columns)
    
    valid_state_mask = master_df['state'].isin(VALID_STATES)
    valid_dist_mask = master_df['district'].isin(VALID_DISTRICTS)
    
    trusted_df = master_df[valid_state_mask & valid_dist_mask]
    
    if not trusted_df.empty:
        # Pincode -> State (Majority Vote)
        pincode_state_map = trusted_df.groupby('pincode')['state'].agg(lambda x: x.value_counts().idxmax()).to_dict()
        
        # Pincode -> District (Majority Vote)
        pincode_dist_map = trusted_df.groupby('pincode')['district'].agg(lambda x: x.value_counts().idxmax()).to_dict()
        
        # Apply Recovery
        
        # 1. Recover States
        # If state is NOT valid, try pincode map
        mask_bad_state = ~master_df['state'].isin(VALID_STATES)
        master_df.loc[mask_bad_state, 'state'] = master_df.loc[mask_bad_state, 'pincode'].map(pincode_state_map).fillna(master_df.loc[mask_bad_state, 'state'])
        
        # 2. Recover Districts
        # If district is 'Unknown' or None, try pincode map
        mask_bad_dist = (master_df['district'] == 'Unknown') | (master_df['district'].isna())
        master_df.loc[mask_bad_dist, 'district'] = master_df.loc[mask_bad_dist, 'pincode'].map(pincode_dist_map).fillna(master_df.loc[mask_bad_dist, 'district'])
        
        print("Pincode Recovery Complete.")
    else:
        print("Warning: Not enough trusted data for Pincode Recovery.")

    # 4. Final Strict Filter: Keep only Valid States
    print("Filtering invalid states...")
    before_count = len(master_df)
    # Filter only rows where state is in VALID_STATES
    master_df = master_df[master_df['state'].isin(VALID_STATES)]
    dropped_count = before_count - len(master_df)
    if dropped_count > 0:
        print(f"Dropped {dropped_count} rows with invalid/garbage state names.")

    print(f"Unique States after Normalization: {master_df['state'].nunique()}")
    print(f"Unique Districts after Normalization: {master_df['district'].nunique()}")
    
    return master_df

if __name__ == "__main__":
    print("Starting Aadhaar Data Processing Pipeline...")
    
    # Integrate
    master_df = integrate_datasets()
    
    # Normalize
    master_df = apply_strict_normalization(master_df)
    
    # Final cleanup of columns if needed
    
    # Save
    output_path = "public/master_dataset_final.csv"
    print(f"Saving Master Dataset to {output_path}...")
    master_df.to_csv(output_path, index=False)
    print("Processing Complete.")
