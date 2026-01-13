---
description: Check notebooks for changes, update cleaning logic, reprocess datasets, and update GitHub release. Triggered by 'CHECK'.
---

1. **Scan Notebooks**:
   - Check `notebooks/` for modifications or new files (e.g., `Aadhar_Demographic_Analysis.ipynb`).
   - Read the notebooks to identify any new cleaning rules (state mappings, district aliases, filters).

2. **Update Cleaning Logic**:
   - if new rules are found, update `app/utils/cleaning_utils.py`.
   - Ensure `clean_dataframe` function reflects the latest logic.

3. **Update Notebook Source**:
   - Run `python3 scripts/auto_update_notebook_source.py` (you might need to recreate this script if it was deleted, or check if the notebook manually needs the API endpoint update).
   - Ensure notebooks are reading from `http://localhost:8000/api/datasets/...`.

4. **Reprocess Datasets**:
   - Run `python3 scripts/reprocess_datasets.py --datasets <dataset_name>` (e.g., `demographic`).
   - Use `--all` if logic affects all datasets.

5. **Update GitHub Release**:
   - Upload the re-processed CSVs to the `dataset-latest` release.
   - Command: `gh release upload dataset-latest public/datasets/<name>_full.csv public/datasets/split_data/<name>_*.csv --clobber`

6. **Commit Changes**:
   - `git add .`
   - `git commit -m "Update cleaning logic and reprocess datasets based on notebook changes"`
   - `git push origin main`
