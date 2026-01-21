import subprocess
import os
import sys

# Repo name for storage
STORAGE_REPO = "sreecharan-desu/uidai-data-storage"

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
            sys.exit(1)

def upload_to_release(file_path, tag_name="dataset-raw"):
    """Uploads a file to a specific release tag in the storage repo."""
    if not os.path.exists(file_path):
        print(f"Warning: File not found: {file_path}")
        return

    create_release_if_not_exists(tag_name, f"Dataset ({tag_name})", f"Automated upload for {tag_name}")

    print(f"Uploading {os.path.basename(file_path)} to {STORAGE_REPO} @ {tag_name}...")
    try:
        subprocess.run([
            "gh", "release", "upload", tag_name, file_path, 
            "--repo", STORAGE_REPO, 
            "--clobber"
        ], check=True)
        print(f"✅ Uploaded {os.path.basename(file_path)}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to upload {file_path}: {e}")
        # Don't exit, allowing other files to try

def download_from_release(filename, output_dir, tag_name="dataset-raw"):
    """Downloads a file from a release."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    print(f"Downloading {filename} from {STORAGE_REPO} @ {tag_name}...")
    try:
        subprocess.run([
            "gh", "release", "download", tag_name, 
            "--repo", STORAGE_REPO, 
            "--pattern", filename,
            "--dir", output_dir,
            "--clobber"
        ], check=True)
        print(f"✅ Downloaded {filename}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to download {filename}: {e}")
        sys.exit(1)
