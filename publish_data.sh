#!/bin/bash

# Configuration
TAG="dataset-latest"
TITLE="Latest Synced Datasets"
NOTES="Automated sync on $(date)"
FILES_DIR="public/datasets"
SPLIT_DIR="public/datasets/split_data"

echo "🚀 Starting Smart Data Deployment..."

# Check dependencies
if ! command -v gh &> /dev/null; then
    echo "❌ Error: 'gh' cli not installed."
    exit 1
fi
if ! command -v sha256sum &> /dev/null && ! command -v shasum &> /dev/null; then
    echo "❌ Error: sha256sum or shasum not installed."
    exit 1
fi

# Define hash function wrapper
calc_hash() {
    if command -v sha256sum &> /dev/null; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

# Ensure we are logged in
if ! gh auth status &> /dev/null; then
    echo "❌ PLease login: gh auth login"
    exit 1
fi

# 1. Check if release exists
if ! gh release view "$TAG" &> /dev/null; then
    echo "⚠️  Release '$TAG' not found. Creating it..."
    gh release create "$TAG" --title "$TITLE" --notes "$NOTES"
fi

# 2. Get Remote Assets List and basic metadata
echo "🔍 Checking remote assets..."
# We download the 'checksums.sha256' file from the release if it exists used for tracking
gh release download "$TAG" -p "checksums.sha256" --dir "$FILES_DIR" --clobber 2>/dev/null || true

LOCAL_CHECKSUMS_FILE="$FILES_DIR/checksums.sha256"
NEW_CHECKSUMS_FILE="$FILES_DIR/checksums_new.sha256"

# Create/Empty new checksums file
> "$NEW_CHECKSUMS_FILE"

files_to_upload=()

# Process Main CSVs
for f in "$FILES_DIR"/*.csv "$SPLIT_DIR"/*.csv; do
    [ -e "$f" ] || continue
    fname=$(basename "$f")
    
    # Calculate local hash
    echo -n "   Hashing $fname ... "
    local_hash=$(calc_hash "$f")
    echo "$local_hash  $fname" >> "$NEW_CHECKSUMS_FILE"
    
    # Check against old checksums
    remote_hash=""
    if [ -f "$LOCAL_CHECKSUMS_FILE" ]; then
        remote_hash=$(grep "$fname" "$LOCAL_CHECKSUMS_FILE" | awk '{print $1}')
    fi
    
    if [ "$local_hash" == "$remote_hash" ]; then
        echo "✅ Unchanged."
    else
        echo "🔄 CHANGED/NEW. Will upload."
        files_to_upload+=("$f")
    fi
done

# 3. Upload Changed Files
if [ ${#files_to_upload[@]} -eq 0 ]; then
    echo "🎉 No changes detected. Everything is up to date."
else
    echo "📤 Uploading ${#files_to_upload[@]} updated files..."
    for f in "${files_to_upload[@]}"; do
        echo "   Uploading $(basename "$f")..."
        gh release upload "$TAG" "$f" --clobber
    done
    
    # Upload new checksums
    echo "   Updating checksum manifest..."
    mv "$NEW_CHECKSUMS_FILE" "$LOCAL_CHECKSUMS_FILE"
    gh release upload "$TAG" "$LOCAL_CHECKSUMS_FILE" --clobber
    
    echo "✅ Update Complete!"
fi

# Cleanup
rm -f "$FILES_DIR/checksums.sha256" 2>/dev/null
