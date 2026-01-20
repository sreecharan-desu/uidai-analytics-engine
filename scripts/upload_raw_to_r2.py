import boto3
import os
import sys
from botocore.config import Config

def upload_raw_to_r2():
    # R2 Credentials
    r2_account_id = os.getenv("R2_ACCOUNT_ID")
    r2_access_key = os.getenv("R2_ACCESS_KEY_ID")
    r2_secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
    r2_bucket_name = os.getenv("R2_BUCKET_NAME")

    if not all([r2_account_id, r2_access_key, r2_secret_key, r2_bucket_name]):
        print("Error: R2 credentials missing.")
        sys.exit(1)

    print(f"Connecting to Cloudflare R2 Bucket: {r2_bucket_name}...")
    
    s3_client = boto3.client(
        's3',
        endpoint_url=f"https://{r2_account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=r2_access_key,
        aws_secret_access_key=r2_secret_key,
        config=Config(signature_version='s3v4')
    )

    # Raw Datasets to upload
    files_to_upload = [
        "public/datasets/enrollment.csv",
        "public/datasets/demographic.csv",
        "public/datasets/biometric.csv"
    ]

    print("Starting upload of RAW datasets...")
    for file_path in files_to_upload:
        if not os.path.exists(file_path):
            print(f"Warning: File not found: {file_path}")
            continue
            
        file_name = os.path.basename(file_path) # e.g. "enrollment.csv"
        # We upload them to the root or a folder. Let's keep them in root for now as per previous schema, 
        # or maybe a 'raw/' folder? The user didn't specify. 
        # But wait, the processing script downloads from release using pattern "*.csv". 
        # If we change storage, we need to ensure the processing script can find them.
        # For now, just upload them with their filenames.
        
        print(f"Uploading {file_name}...")
        try:
            s3_client.upload_file(
                file_path, 
                r2_bucket_name, 
                file_name,
                ExtraArgs={'ContentType': 'text/csv'}
            )
            print(f"✅ Uploaded {file_name}")
        except Exception as e:
            print(f"❌ Failed to upload {file_name}: {e}")
            sys.exit(1)

    print("All uploads complete.")

if __name__ == "__main__":
    upload_raw_to_r2()
