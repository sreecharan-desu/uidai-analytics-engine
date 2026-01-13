
# Pre-computed map for O(1) case-insensitive lookup

# Pre-computed map for O(1) case-insensitive lookup
STATE_STANDARD_MAP = {
  'andhra pradesh': 'Andhra Pradesh',
  'arunachal pradesh': 'Arunachal Pradesh',
  'assam': 'Assam',
  'bihar': 'Bihar',
  'chhattisgarh': 'Chhattisgarh',
  'chhatisgarh': 'Chhattisgarh',
  'goa': 'Goa',
  'gujarat': 'Gujarat',
  'haryana': 'Haryana',
  'himachal pradesh': 'Himachal Pradesh',
  'jharkhand': 'Jharkhand',
  'karnataka': 'Karnataka',
  'kerala': 'Kerala',
  'madhya pradesh': 'Madhya Pradesh',
  'maharashtra': 'Maharashtra',
  'manipur': 'Manipur',
  'meghalaya': 'Meghalaya',
  'mizoram': 'Mizoram',
  'nagaland': 'Nagaland',
  'odisha': 'Odisha',
  'orissa': 'Odisha',
  'punjab': 'Punjab',
  'rajasthan': 'Rajasthan',
  'sikkim': 'Sikkim',
  'tamil nadu': 'Tamil Nadu',
  'tamilnadu': 'Tamil Nadu',
  'telangana': 'Telangana',
  'tripura': 'Tripura',
  'uttar pradesh': 'Uttar Pradesh',
  'uttarakhand': 'Uttarakhand',
  'uttaranchal': 'Uttarakhand',
  'west bengal': 'West Bengal',
  'westbengal': 'West Bengal',
  'west bangal': 'West Bengal',
  'west bengli': 'West Bengal',
  'west bengal.': 'West Bengal',
  # UTs
  'andaman and nicobar islands': 'Andaman and Nicobar Islands',
  'andaman nicobar islands': 'Andaman and Nicobar Islands',
  'chandigarh': 'Chandigarh',
  'dadra and nagar haveli and daman and diu': 'Dadra and Nagar Haveli and Daman and Diu',
  'the dadra and nagar haveli and daman and diu': 'Dadra and Nagar Haveli and Daman and Diu',
  'dadra nagar haveli': 'Dadra and Nagar Haveli and Daman and Diu',
  'daman and diu': 'Dadra and Nagar Haveli and Daman and Diu',
  'daman & diu': 'Dadra and Nagar Haveli and Daman and Diu',
  'daman diu': 'Dadra and Nagar Haveli and Daman and Diu',
  'dadra and nagar haveli': 'Dadra and Nagar Haveli and Daman and Diu',
  'dadra & nagar haveli': 'Dadra and Nagar Haveli and Daman and Diu',
  'delhi': 'Delhi',
  'new delhi': 'Delhi',
  'jammu and kashmir': 'Jammu and Kashmir',
  'jammu kashmir': 'Jammu and Kashmir',
  'jammu & kashmir': 'Jammu and Kashmir',
  'ladakh': 'Ladakh',
  'lakshadweep': 'Lakshadweep',
  'puducherry': 'Puducherry',
  'pondicherry': 'Puducherry',
  
  # Cities/Districts appearing as States
  'nagpur': 'Maharashtra',
  'jaipur': 'Rajasthan',
  'gurgaon': 'Haryana',
  'pune city': 'Maharashtra',
  'darbhanga': 'Bihar',
  'madanapalle': 'Andhra Pradesh',
  'balanagar': 'Telangana',
  'puttenahalli': 'Karnataka',
  'raja annamalai puram': 'Tamil Nadu',
  'greater kailash 2': 'Delhi',
  'puthur': 'Andhra Pradesh',
  '100000': 'Unknown', # Explicitly mark garbage
  '561203': 'Karnataka', # Pincode for Bangalore rural/urban
}

VALID_STATES = {
  'Andhra Pradesh', 'Arunachal Pradesh', 'Assam', 'Bihar', 'Chhattisgarh',
  'Goa', 'Gujarat', 'Haryana', 'Himachal Pradesh', 'Jharkhand',
  'Karnataka', 'Kerala', 'Madhya Pradesh', 'Maharashtra', 'Manipur',
  'Meghalaya', 'Mizoram', 'Nagaland', 'Odisha', 'Punjab',
  'Rajasthan', 'Sikkim', 'Tamil Nadu', 'Telangana', 'Tripura',
  'Uttar Pradesh', 'Uttarakhand', 'West Bengal',
  'Andaman and Nicobar Islands', 'Chandigarh',
  'Dadra and Nagar Haveli and Daman and Diu',
  'Delhi', 'Jammu and Kashmir', 'Ladakh', 'Lakshadweep', 'Puducherry'
}

LOWER_CASE_VALID_STATES = {s.lower(): s for s in VALID_STATES}

DISTRICT_ALIAS_MAP = {
    # Tamil Nadu
    'tuticorin': 'Thoothukkudi',

    # Karnataka
    'bangalore': 'Bengaluru',
    'belgaum': 'Belagavi',
    'shimoga': 'Shivamogga',
    'mysore': 'Mysuru',
    'tumkur': 'Tumakuru',
    'bellary': 'Ballari',

    # Maharashtra / MP
    'gondia': 'Gondiya',
    'ahmadabad': 'Ahmedabad',
    'ahmadnagar': 'Ahilyanagar',
    'ahmed nagar': 'Ahilyanagar',
    'hoshangabad': 'Narmadapuram',
    'mumbai( sub urban )': 'Mumbai Suburban',

    # West Bengal
    'burdwan': 'Bardhaman',
    'barddhaman': 'Bardhaman',
    'hugli': 'Hooghly',
    'hooghiy': 'Hooghly',
    'hawrah': 'Howrah',
    'haora': 'Howrah',
    
    # Uttarakhand
    'hardwar': 'Haridwar',

    # Uttar Pradesh
    'allahabad': 'Prayagraj',

    # Andhra Pradesh
    'ysr': 'Y.S.R. Kadapa',
    'y s r': 'Y.S.R. Kadapa',
    'y.s.r.': 'Y.S.R. Kadapa',
    'y. s. r': 'Y.S.R. Kadapa',
    'cuddapah': 'Y.S.R. Kadapa',
    'anantapur': 'Ananthapuramu',
    'ananthapur': 'Ananthapuramu',

    # Telangana / AP Split fallout
    'k v rangareddy': 'Rangareddy',
    'k v rangareddi': 'Rangareddy',
    'k.v.rangareddy': 'Rangareddy',
    'k.v. rangareddy': 'Rangareddy',
    'rangareddi': 'Rangareddy',
    'warangal (urban)': 'Warangal Urban',
    'karim nagar': 'Karimnagar',
    'medchal malkajgiri': 'Medchal-Malkajgiri',

    # Gujarat
    'banas kantha': 'Banaskantha',

    # HP
    'lahul spiti': 'Lahaul And Spiti',
}
