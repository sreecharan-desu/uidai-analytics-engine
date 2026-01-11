# PowerBI Visualization Guide for UIDAI Analytics

This guide walks you through connecting PowerBI to the UIDAI Analytics API and creating a comprehensive dashboard that matches the insights from the Python notebooks (State-wise, Age-wise, and Monthly Trends).

---

## **Part 1: Connecting PowerBI to the API**

Repeat these steps for **each** dataset you want to visualize (`enrolment`, `biometric`, or `demographic`).

1.  Open **Power BI Desktop**.
2.  Click **Get Data** (Home ribbon) > **Web**.
3.  Enter the API URL for the dataset:
    *   **Enrolment**: `https://uidai.sreecharandesu.in/api/analytics/enrolment?year=2025`
    *   **Biometric**: `https://uidai.sreecharandesu.in/api/analytics/biometric?year=2025`
4.  Click **OK**. If asked for authentication, choose **Anonymous** and **Connect**.
5.  The Power Query Editor window will open. You will see a list of records.
6.  Look for the `data` field. It will say `[Record]`. **Click the word "Record"** (or the small expand icon if visible) to drill down into the actual data.

You should now see three key records:
*   `by_state`
*   `by_age_group`
*   `by_month`

---

## **Part 2: Preparing the Data Tables**

Since PowerBI needs separate tables for different visuals (Map vs Trend), we will duplicate this query three times.

### **Step A: Create "State Analysis" Table**
1.  On the left sidebar ("Queries"), right-click your query (e.g., `enrolment`) and select **Duplicate**.
2.  Rename the new query to **Enrolment_State**.
3.  In the main view, find the `by_state` record. Click on **Record** (or right-click > Drill Down).
4.  You will see a list of states. Go to the **Transform** tab (top menu) > Click **To Table**.
5.  Click OK.
6.  Rename the columns:
    *   `Name` -> **State**
    *   `Value` -> **Count**
7.  **Important**: Click the icon "ABC/123" on the **Count** column header and change type to **Whole Number**.

### **Step B: Create "Age Analysis" Table**
1.  Go back to the original query (or duplicate the original again). Rename to **Enrolment_Age**.
2.  Find the `by_age_group` record. Drill down into it.
3.  **Transform** > **To Table**.
4.  Rename columns:
    *   `Name` -> **Age Group**
    *   `Value` -> **Count**
5.  Change **Count** to **Whole Number**.

### **Step C: Create "Trend Analysis" Table**
1.  Duplicate the original query again. Rename to **Enrolment_Trend**.
2.  Find the `by_month` record. Drill down into it.
3.  **Transform** > **To Table**.
4.  Rename columns:
    *   `Name` -> **Month**
    *   `Value` -> **Count**
5.  Change **Count** to **Whole Number**.
6.  (Optional) If Month is just "01", "02", you might want to create a new column to map it to "Jan", "Feb", etc., or ensuring it sorts numerically.

**Click "Close & Apply"** (Top Left) to load data into the Report View.

---

## **Part 3: Building the Visuals**

Now drag and drop the fields to build your dashboard.

### **1. Choropleth Map (State-wise Load)**
*   **Visual Type**: Filled Map (or Azure Map).
*   **Data Source**: `Enrolment_State` table.
*   **Location**: Drag `State` field here.
*   **Tooltips/Color Saturation**: Drag `Count` field here.
*   **Insight**: Shows high-activity states (UP, Bihar, Maharashtra) in darker colors.

### **2. Donut Chart (Age Distribution)**
*   **Visual Type**: Donut Chart.
*   **Data Source**: `Enrolment_Age` table.
*   **Legend**: Drag `Age Group`.
*   **Values**: Drag `Count`.
*   **Insight**: Shows the split between Children (0-5), Students (5-17), and Adults (18+).

### **3. Line Chart (Monthly Trend)**
*   **Visual Type**: Line Chart.
*   **Data Source**: `Enrolment_Trend` table.
*   **X-Axis**: Drag `Month`.
*   **Y-Axis**: Drag `Count`.
*   **Insight**: Reveals spikes in enrolment campaigns or seasonal trends.

### **4. KPI Card (Total)**
*   **Visual Type**: Card.
*   **Data Source**: Any table (e.g., `Enrolment_State`).
*   **Fields**: Drag `Count`.
*   **Note**: PowerBI will automatically Sum them up to give the grand total.

---

## **Part 4: Publishing**
1.  Arrange the titles and colors to look professional.
2.  Click **Publish** on the top right.
3.  Sign in to your PowerBI Service account to share the web link.
