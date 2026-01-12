# Visualization Options for Mac

You can visualize the UIDAI Analytics data on your Mac using several methods, ranging from easiest to most advanced.

## 1. The Instant Web Dashboard (Recommended)
We have built a custom, lightweight dashboard directly into this project.
- **How to use**: 
  1. Ensure the server is running (`npm run dev`).
  2. Open **[http://localhost:3000/dashboard.html](http://localhost:3000/dashboard.html)** in your browser.
- **Features**: Interactive charts, dark mode, no installation required.

## 2. Spreadsheet Software (Excel / Numbers)
Mac comes with **Numbers**, and you likely have **Excel**.
- **How to use**:
  1. Download the CSV data manually:
     - [Biometric 2025 CSV](http://localhost:3000/api/analytics/biometric?year=2025&format=csv)
     - [Enrolment 2025 CSV](http://localhost:3000/api/analytics/enrolment?year=2025&format=csv)
  2. Double-click the downloaded .csv file.
  3. Select columns and click "Insert Chart".

## 3. Google Sheets (Cloud)
- **How to use**:
  1. Go to `sheets.new`.
  2. File > Import > Upload (Upload the CSV).
  3. Select data > Insert > Chart.

## 4. Jupyter Notebook (Advanced)
If you prefer Python:
1. Open the `notebooks/` directory.
2. Create a new notebook using `pandas` and `matplotlib` to fetch and plot data.

## 5. PowerBI (Virtual Machine)
If you strictly need PowerBI, you must run Windows via Parallels or UTM, as PowerBI Desktop is not available for Mac.
