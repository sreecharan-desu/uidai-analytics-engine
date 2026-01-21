import sys
import os

# Add scripts directory to path to import utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from github_utils import download_from_release

def download_raw_data():
    files = ["biometric.csv", "enrollment.csv", "demographic.csv", "enrolment.csv"]
    output_dir = "public/datasets"
    
    print("Starting download of raw datasets from GitHub...")
    for f in files:
        # We try both enrollment spellings just in case
        try:
             download_from_release(f, output_dir, tag_name="dataset-raw")
        except Exception as e:
            print(f"Note: Could not download {f}, might be optional or named differently.")

if __name__ == "__main__":
    download_raw_data()
