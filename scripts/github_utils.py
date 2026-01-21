import subprocess
import os
import sys
import time

# Repo name for storage
STORAGE_REPO = "sreecharan-desu/uidai-data-storage"

def retry_command(cmd, max_retries=3, delay=5):
    """Retries a subprocess command several times."""
    for i in range(max_retries):
        try:
            subprocess.run(cmd, check=True)
            return True
        except subprocess.CalledProcessError as e:
            print(f"Attempt {i+1}/{max_retries} failed: {e}")
            if i < max_retries - 1:
                time.sleep(delay * (i + 1))
            else:
                return False
    return False

def check_gh_auth():
    """Checks if gh cli is authenticated."""
    try:
        subprocess.run(["gh", "auth", "status"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError:
        print("Error: gh CLI not authenticated. Please ensure you have set up a GH_PAT secret or are logged in.")
        return False

def create_release_if_not_exists(tag_name, title, body="Automated release"):
    """Creates a release in the storage repo if it doesn't exist."""
    print(f"Checking for release '{tag_name}' in {STORAGE_REPO}...")
    try:
        # Check if release exists
        subprocess.run(["gh", "release", "view", tag_name, "--repo", STORAGE_REPO], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"Release '{tag_name}' exists.")
    except subprocess.CalledProcessError:
        print(f"Creating release '{tag_name}'...")
        try:
            subprocess.run([
                "gh", "release", "create", tag_name, 
                "--repo", STORAGE_REPO, 
                "--title", title, 
                "--notes", body
            ], check=True)
            print(f"Release '{tag_name}' created successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Failed to create release: {e}")
            # We don't exit here because another process might have created it simultaneously
            # The upload step will confirm if it works

def upload_to_release(file_path, tag_name="dataset-raw"):
    """Uploads a file to a specific release tag in the storage repo with retries."""
    if not os.path.exists(file_path):
        print(f"Warning: File not found: {file_path}")
        return False

    create_release_if_not_exists(tag_name, f"Dataset ({tag_name})", f"Automated upload for {tag_name}")

    print(f"Uploading {os.path.basename(file_path)} to {STORAGE_REPO} @ {tag_name}...")
    
    cmd = [
        "gh", "release", "upload", tag_name, file_path, 
        "--repo", STORAGE_REPO, 
        "--clobber"
    ]
    
    if retry_command(cmd):
        print(f"✅ Successfully uploaded {os.path.basename(file_path)}")
        return True
    else:
        print(f"❌ Failed to upload {file_path} after multiple attempts.")
        return False

def download_from_release(filename, output_dir, tag_name="dataset-raw"):
    """Downloads a file from a release with retries."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    print(f"Downloading {filename} from {STORAGE_REPO} @ {tag_name}...")
    
    cmd = [
        "gh", "release", "download", tag_name, 
        "--repo", STORAGE_REPO, 
        "--pattern", filename,
        "--dir", output_dir,
        "--clobber"
    ]
    
    if retry_command(cmd):
        print(f"✅ Successfully downloaded {filename}")
        return True
    else:
        print(f"❌ Failed to download {filename} after multiple attempts.")
        return False
