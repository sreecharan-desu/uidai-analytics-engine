import boto3
import os
import sys
from botocore.config import Config

def upload_to_r2(file_path):
    """
    Uploads a single file to Cloudflare R2.
    """
    # R2 Credentials
    r2_account_id = os.getenv("R2_ACCOUNT_ID")
    r2_access_key = os.getenv("R2_ACCESS_KEY_ID")
    r2_secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
    r2_bucket_name = os.getenv("R2_BUCKET_NAME")

    if not all([r2_account_id, r2_access_key, r2_secret_key, r2_bucket_name]):
        print("Error: R2 credentials missing.")
        sys.exit(1)

    if not os.path.exists(file_path):
        print(f"Warning: File not found: {file_path}")
        return

    print(f"Connecting to Cloudflare R2 Bucket: {r2_bucket_name}...")
    
    s3_client = boto3.client(
        's3',
        endpoint_url=f"https://{r2_account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=r2_access_key,
        aws_secret_access_key=r2_secret_key,
        config=Config(signature_version='s3v4')
    )
    
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
        # We don't exit here to allow calling script to handle logic if needed, 
        # but for this usage we can just print error.
