
import os
import sys
import pandas as pd
import argparse

# Add project root to path
sys.path.append(os.getcwd())

from app.utils.cleaning_utils import clean_dataframe

DATASETS = ['enrolment', 'biometric', 'demographic']

def reprocess_dataset(name):
    print(f"--- Reprocessing {name} ---")
    file_path = f"public/datasets/{name}_full.csv"
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return False

    print("Loading data...")
    try:
        df = pd.read_csv(file_path, low_memory=False)
        original_count = len(df)
        print(f"Original Records: {original_count}")
        
        # Apply Cleaning
        df_clean = clean_dataframe(df, name)
        new_count = len(df_clean)
        print(f"Cleaned Records: {new_count} (Dropped {original_count - new_count})")
        
        # Save Full
        print("Saving full cleaned file...")
        if 'year' in df_clean.columns:
            save_df = df_clean.drop(columns=['year'])
        else:
            save_df = df_clean
        save_df.to_csv(file_path, index=False)
        
        # Split by Year
        if 'year' in df_clean.columns:
            split_dir = "public/datasets/split_data"
            os.makedirs(split_dir, exist_ok=True)
            print("Splitting by year...")
            
            for year, group in df_clean.groupby('year'):
                out_path = os.path.join(split_dir, f"{name}_{year}.csv")
                group.drop(columns=['year']).to_csv(out_path, index=False)
                # print(f"  Saved {year}: {len(group)}")
                
        print(f"✅ Reprocess complete for {name}")
        return True
        
    except Exception as e:
        print(f"❌ Error processing {name}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Reprocess local datasets via cleaning rules.")
    parser.add_argument('datasets', nargs='*', default=[], help="Specific datasets to process (e.g. demographic enrolment)")
    parser.add_argument('--all', action='store_true', help="Process all known datasets")
    
    args = parser.parse_args()
    
    targets = []
    if args.all:
        targets = DATASETS
    elif args.datasets:
        targets = args.datasets
    else:
        print("Please specify datasets to process or --all")
        print(f"Available: {DATASETS}")
        return

    for t in targets:
        if t not in DATASETS:
            print(f"Skipping unknown dataset: {t}")
            continue
        reprocess_dataset(t)

if __name__ == "__main__":
    main()
