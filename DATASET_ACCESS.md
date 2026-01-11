# UIDAI Aadhaar Datasets - Access Links

This document provides direct download links for the synced UIDAI Aadhaar datasets. These datasets are automatically synchronized daily from `data.gov.in` and processed for consistency.

**Base URL:** `https://uidai.sreecharandesu.in`

---

## Full Datasets (Aggregated)
These files contain the complete history available in the system.

| Dataset | Description | Download Link |
| :--- | :--- | :--- |
| **Enrolment** | New Aadhaar generation stats by age group. | [Download CSV](https://uidai.sreecharandesu.in/api/datasets/enrolment) |
| **Biometric** | Biometric update stats (iris/fingerprint) by age. | [Download CSV](https://uidai.sreecharandesu.in/api/datasets/biometric) |
| **Demographic** | Profile update stats (name/address/mobile) by age. | [Download CSV](https://uidai.sreecharandesu.in/api/datasets/demographic) |

---

## Year-wise Datasets (Split)
Optimized smaller files split by calendar year.

### 2025 Data
| Dataset | Year | Link |
| :--- | :--- | :--- |
| Enrolment | 2025 | [Download 2025 CSV](https://uidai.sreecharandesu.in/api/datasets/enrolment?year=2025) |
| Biometric | 2025 | [Download 2025 CSV](https://uidai.sreecharandesu.in/api/datasets/biometric?year=2025) |
| Demographic | 2025 | [Download 2025 CSV](https://uidai.sreecharandesu.in/api/datasets/demographic?year=2025) |

### 2026 Data
| Dataset | Year | Link |
| :--- | :--- | :--- |
| Enrolment | 2026 | [Download 2026 CSV](https://uidai.sreecharandesu.in/api/datasets/enrolment?year=2026) |
| Biometric | 2026 | [Download 2026 CSV](https://uidai.sreecharandesu.in/api/datasets/biometric?year=2026) |
| Demographic | 2026 | [Download 2026 CSV](https://uidai.sreecharandesu.in/api/datasets/demographic?year=2026) |

---

## Usage
You can download these files directly in your browser or use command line tools:

**Using curl:**
```bash
# Download Full Enrolment Data
curl -O https://uidai.sreecharandesu.in/api/datasets/enrolment

# Download Specific Year
curl -o biometric_2025.csv "https://uidai.sreecharandesu.in/api/datasets/biometric?year=2025"
```

**Using Python:**
```python
import pandas as pd

url = "https://uidai.sreecharandesu.in/api/datasets/enrolment?year=2025"
df = pd.read_csv(url)
print(df.head())
```
