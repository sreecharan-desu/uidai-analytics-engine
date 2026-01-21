import sys
import os

# Add scripts directory to path to import utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from github_utils import upload_to_release

def upload_processed_data():
    files_to_upload = [
        "public/master_dataset_final.csv",
        "public/datasets/biometric_full.csv",
        "public/datasets/enrollment_full.csv",
        "public/datasets/demographic_full.csv"
    ]
    
    print("Starting upload of processed datasets to GitHub...")
    for file_path in files_to_upload:
        if os.path.exists(file_path):
            upload_to_release(file_path, tag_name="dataset-latest")
        else:
            print(f"Warning: File not found {file_path}")

if __name__ == "__main__":
    upload_processed_data()
