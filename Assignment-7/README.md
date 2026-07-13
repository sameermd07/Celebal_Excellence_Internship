# Week 7 Assignment - Delta Lake MERGE using Databricks

## Objective

Implement incremental data processing using **Delta Lake MERGE** in **Databricks**. This assignment demonstrates how to create a Delta table, clean the data, process incremental records, and perform update and insert operations using the `MERGE` command.

---

## Dataset

- **Sample - Superstore.csv** – Original Superstore dataset containing **9,994 records**.

---

## Project Structure

```
week-7-assignment/
│
├── data/
│   └── Sample - Superstore.csv
│
├── notebooks/
│   └── delta_merge_assignment.ipynb
│
├── screenshots/
│   ├── data_loading/
│   ├── delta_table/
│   ├── data_cleaning/
│   ├── merge_operation/
│   ├── validation/
│   └── final_output/
│
└── README.md
```

---

## What I Did

### 1. Loaded the Dataset

- Loaded the **Sample - Superstore.csv** file from Unity Catalog Volume using PySpark.
- Verified the dataset by displaying sample records.

### 2. Created a Delta Table

- Renamed column names by replacing spaces with underscores.
- Stored the dataset as a Delta table.

### 3. Cleaned the Data

- Removed duplicate records.
- Filled null values in **Category** and **Sub_Category** with `"Unknown"`.
- Filled remaining null values with `0`.
- Saved the cleaned data back into the Delta table.

### 4. Created an Incremental Dataset

- Created a small incremental dataset containing:
  - Existing records to be updated.
  - A new record to be inserted.

### 5. Performed Delta MERGE

- Used **Order_ID** as the matching key.
- Updated existing records using `whenMatchedUpdate`.
- Inserted new records using `whenNotMatchedInsert`.

### 6. Validated the Results

- Displayed the updated Delta table.
- Verified the total number of records after the merge.
- Confirmed that updated and newly inserted records were reflected correctly.

---

## Technologies Used

- Databricks
- PySpark
- Delta Lake
- Unity Catalog
- CSV Files

---

## How to Run

1. Upload **Sample - Superstore.csv** to Unity Catalog Volume.
2. Import the notebook into Databricks.
3. Attach a Spark cluster.
4. Run all notebook cells in sequence.
5. Capture the required screenshots.

---

## Output

- Successfully created a Delta table.
- Cleaned duplicate and null records.
- Performed Delta Lake MERGE operation.
- Updated existing records.
- Inserted new records.
- Validated the final dataset successfully.