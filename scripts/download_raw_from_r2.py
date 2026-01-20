import boto3
import os
import sys
from botocore.config import Config

def download_raw_from_r2():
    # R2 Credentials
    r2_account_id = os.getenv("R2_ACCOUNT_ID")
    r2_access_key = os.getenv("R2_ACCESS_KEY_ID")
    r2_secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
    r2_bucket_name = os.getenv("R2_BUCKET_NAME")

    if not all([r2_account_id, r2_access_key, r2_secret_key, r2_bucket_name]):
        print("Error: R2 credentials missing.")
        sys.exit(1)

    print(f"Connecting to Cloudflare R2 Bucket: {r2_bucket_name} for download...")
    
    s3_client = boto3.client(
        's3',
        endpoint_url=f"https://{r2_account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=r2_access_key,
        aws_secret_access_key=r2_secret_key,
        config=Config(signature_version='s3v4')
    )

    # Use 'enrolment.csv' as standard now
    files_to_download = [
        "enrolment.csv",
        "demographic.csv",
        "biometric.csv"
    ]
    
    os.makedirs("public/datasets", exist_ok=True)

    for file_name in files_to_download:
        output_path = os.path.join("public/datasets", file_name)
        print(f"Downloading {file_name} from R2...")
        
        try:
            s3_client.download_file(
                r2_bucket_name, 
                file_name, 
                output_path
            )
            print(f"✅ Downloaded {file_name}")
        except Exception as e:
            # Fallback for spelling variations if needed
            if file_name == "enrolment.csv":
                try:
                    print(f"Retrying with 'enrollment.csv'...")
                    s3_client.download_file(r2_bucket_name, "enrollment.csv", output_path)
                    print(f"✅ Downloaded enrollment.csv as {file_name}")
                    continue
                except:
                    pass
            print(f"❌ Failed to download {file_name}: {e}")
            sys.exit(1)

    print("All raw downloads complete.")

if __name__ == "__main__":
    download_raw_from_r2()
