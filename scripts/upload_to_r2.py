import boto3
import os
import sys
from botocore.config import Config

def upload_to_r2():
    # R2 Credentials
    r2_account_id = os.getenv("R2_ACCOUNT_ID")
    r2_access_key = os.getenv("R2_ACCESS_KEY_ID")
    r2_secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
    r2_bucket_name = os.getenv("R2_BUCKET_NAME")

    if not all([r2_account_id, r2_access_key, r2_secret_key, r2_bucket_name]):
        print("Error: R2 credentials missing (R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME)")
        sys.exit(1)

    print(f"Connecting to Cloudflare R2 Bucket: {r2_bucket_name}...")
    
    s3_client = boto3.client(
        's3',
        endpoint_url=f"https://{r2_account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=r2_access_key,
        aws_secret_access_key=r2_secret_key,
        config=Config(signature_version='s3v4')
    )

    # Files to upload
    files_to_upload = [
        "public/master_dataset_final.csv",
        "public/datasets/biometric_full.csv",
        "public/datasets/enrollment_full.csv",
        "public/datasets/demographic_full.csv"
    ]

    for file_path in files_to_upload:
        if not os.path.exists(file_path):
            print(f"Warning: File not found: {file_path}")
            continue
            
        file_name = os.path.basename(file_path)
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
    upload_to_r2()
