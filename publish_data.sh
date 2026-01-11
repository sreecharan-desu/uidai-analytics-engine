#!/bin/bash

# Configuration
TAG="dataset-latest"
TITLE="Latest Synced Datasets"
NOTES="Automated sync on $(date)"
FILES="public/datasets/*.csv public/datasets/split_data/*.csv"

echo "🚀 Starting Data Deployment to GitHub Releases..."

# Check if gh is installed
if ! command -v gh &> /dev/null; then
    echo "❌ Error: GitHub CLI (gh) is not installed."
    exit 1
fi

# Check if we are logged in
if ! gh auth status &> /dev/null; then
    echo "❌ Error: Not logged in to GitHub CLI. Run 'gh auth login'."
    exit 1
fi

echo "🗑️  Deleting old release..."
gh release delete "$TAG" --yes || true
git push origin :refs/tags/"$TAG" || true
git tag -d "$TAG" || true

echo "📦 Creating new release and uploading files..."
# We use 'ls' to expand the files variable correctly or pass them as args
# Using $FILES directly might fail if no files match (glob expansion).
# Let's rely on shell expansion.

gh release create "$TAG" $FILES --title "$TITLE" --notes "$NOTES"

echo "✅ Data successfully deployed to GitHub Releases!"
echo "🔗 Release URL: https://github.com/sreecharan-desu/uidai-data-sync/releases/tag/$TAG"
