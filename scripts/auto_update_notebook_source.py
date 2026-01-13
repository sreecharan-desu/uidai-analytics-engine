
import os
import json
import re

NOTEBOOKS_DIR = 'notebooks'
API_BASE_URL = 'http://localhost:8000/api/datasets'

DATASET_MAP = {
    'demographic': 'demographic',
    'enrolment': 'enrolment',
    'biometric': 'biometric',
    'aadhaar': 'enrolment' # Fallback
}

def get_api_code(dataset_name):
    return [
        "import pandas as pd\n",
        "import io\n",
        "import requests\n",
        "\n",
        f"dataset_name = '{dataset_name}'\n",
        "api_url = f'http://localhost:8000/api/datasets/{dataset_name}'\n",
        "local_path = f'../public/datasets/{dataset_name}_full.csv'\n",
        "\n",
        "try:\n",
        "    print(f'Attempting to fetch data from API: {api_url}')\n",
        "    response = requests.get(api_url)\n",
        "    response.raise_for_status()\n",
        "    df = pd.read_csv(io.StringIO(response.text))\n",
        "    print('Successfully loaded data from API')\n",
        "except Exception as e:\n",
        "    print(f'API unavailable or failed ({e}). Falling back to local CSV: {local_path}')\n",
        "    try:\n",
        "        df = pd.read_csv(local_path, on_bad_lines='skip', low_memory=False)\n",
        "        print('Successfully loaded data from local CSV')\n",
        "    except FileNotFoundError:\n",
        "        print(f'Error: Local file {local_path} not found. Please ensure data is synced.')\n",
        "        # Create empty dataframe as fallback to prevent crash\n",
        "        df = pd.DataFrame()\n"
    ]

def process_notebook(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        nb = json.load(f)

    modified = False
    
    for cell in nb['cells']:
        if cell['cell_type'] == 'code':
            source = cell['source']
            source_text = "".join(source)
            
            # Detect pd.read_csv calls
            if 'pd.read_csv' in source_text:
                # Heuristic to detect dataset name
                dataset_name = None
                for key in DATASET_MAP:
                    if key in source_text.lower():
                        dataset_name = DATASET_MAP[key]
                        break
                
                if dataset_name and 'http://localhost:8000' not in source_text:
                    print(f"Updating {filepath} to use API for {dataset_name}...")
                    cell['source'] = get_api_code(dataset_name)
                    modified = True

    if modified:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(nb, f, indent=1)
        print(f"Saved updates to {filepath}")
    else:
        print(f"No changes needed for {filepath}")

def main():
    if not os.path.exists(NOTEBOOKS_DIR):
        print(f"Directory {NOTEBOOKS_DIR} not found.")
        return

    for filename in os.listdir(NOTEBOOKS_DIR):
        if filename.endswith('.ipynb'):
            path = os.path.join(NOTEBOOKS_DIR, filename)
            process_notebook(path)

if __name__ == '__main__':
    main()
