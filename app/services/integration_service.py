import requests
import os

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
