# LearnTrack – LMS Analytics Pipeline & Power BI Dashboard

An end-to-end data engineering and analytics project for a Learning Management System (LMS). Raw learner, course, and enrolment data is processed through a **Medallion Architecture (Bronze → Silver → Gold)** in Databricks, validated for quality, analyzed with SQL, and visualized in an interactive **Power BI dashboard**.

---

## 📌 Project Overview

The goal of this project is to help LMS stakeholders answer key business questions such as:

- Which courses have the highest / lowest completion rates?
- Which courses are "at risk" of low completion?
- Who are the top-performing instructors?
- Which learners are disengaged or at risk of dropping out?
- How are learners performing on assessments (scores, pass rates)?

---

## 🏗️ Architecture

```
Raw CSVs  →  Bronze Layer  →  Silver Layer  →  Gold Layer  →  Power BI Dashboard
(source)     (raw ingest)     (cleaned/          (business-
                               deduplicated)       ready aggregates)
```

- **Bronze Layer** – Raw data ingested as-is from source CSVs (`learners`, `courses`, `enrolment_activity`) with schema inference, no transformations.
- **Silver Layer** – Cleaned and deduplicated data (e.g., duplicate enrolments removed, joins between learners/courses/enrolment activity to form a unified `silver_lms` table).
- **Gold Layer** – Business-level aggregate tables built to answer specific questions:
  - `gold_course_completion` – completion rate and risk category per course
  - `gold_assessment_performance` – average score, pass rate per course
  - `gold_instructor_performance` – average score, rating, completion rate, ranking per instructor
  - `gold_learner_engagement` – learner-level engagement status (e.g., Disengaged) based on recency of activity
  - `gold_dropout_reenrolment` – learner-level dropout / re-enrolment detail

Processing is done using **PySpark on Databricks**, with tables built on top of Unity Catalog (`learntrack_lms_analytics.default.*`).

---

## 📂 Repository Structure

```
Final_Project/
│
├── DashBoard.pbix                     # Power BI dashboard (final deliverable)
│
├── Data/
│   ├── Bronze/                        # Raw ingested data
│   │   ├── bronze_learners.csv
│   │   ├── bronze_courses.csv
│   │   └── bronze_enrolment_activity.csv
│   │
│   ├── Silver/                        # Cleaned & deduplicated data
│   │   ├── silver_learners.csv
│   │   ├── silver_courses.csv
│   │   ├── silver_enrolment_activity.csv
│   │   └── silver_lms.csv             # Unified silver table (joined)
│   │
│   └── Gold/                          # Business-ready aggregate tables
│       ├── gold_course_completion.csv
│       ├── gold_assessment_performance.csv
│       ├── gold_instructor_performance.csv
│       ├── gold_learner_engagement.csv
│       └── gold_dropout_reenrolment.csv
│
├── NoteBooks/
│   ├── Bronze_Layer.py                # Ingests raw CSVs into Bronze tables
│   ├── Silver_Layer.py                # Cleans, deduplicates, joins into Silver
│   ├── Gold_Layer.py                  # Builds Gold aggregate/business tables
│   ├── Data_Validation.py             # Row counts, duplicate & PK checks
│   └── sql_analysis.sql               # Ad-hoc SQL business questions on Gold tables
│
├── WorkFlow/
│   ├── 01_pipeline.png                # Databricks workflow/job DAG
│   ├── 02_running.png                 # Pipeline run in progress
│   └── 03_completed.png               # Successful pipeline completion
│
└── Screenshot (2078).png              # Dashboard/project screenshot
```

---

## 🔧 Tech Stack

| Layer               | Tool / Technology            |
|---------------------|-------------------------------|
| Data Processing     | PySpark (Databricks Notebooks) |
| Storage             | Unity Catalog Volumes / Tables |
| Data Validation     | PySpark (row count, duplicate, PK checks) |
| Analysis            | Spark SQL |
| Visualization       | Power BI (`DashBoard.pbix`) |

---

## 🧪 Data Validation

`Data_Validation.py` performs quality checks between the Bronze and Silver layers, including:

1. **Record Count Validation** – compares row counts across Bronze vs. Silver tables.
2. **Duplicate Check** – flags any `enrolment_id` appearing more than once.
3. **Primary Key Check** – confirms key columns are unique and non-null.

---

## 🔍 Key SQL Business Questions (`sql_analysis.sql`)

1. Top 10 courses by completion rate
2. Courses flagged as "At Risk" (low completion rate)
3. Best-performing instructors (by rank, score, completion rate)
4. Disengaged learners (based on days since last activity)
5. Additional cuts on assessment performance and dropout/re-enrolment behavior

---

## 📊 Power BI Dashboard (`DashBoard.pbix`)

The dashboard is built on the **Gold layer** tables and includes visuals for:

- **Course Completion** – completion rate by course/category, at-risk course flags
- **Assessment Performance** – average score & pass rate by course
- **Instructor Performance** – ranked leaderboard of instructors by score, rating, and completion rate
- **Learner Engagement** – engagement status breakdown (e.g., Active vs. Disengaged)
- **Dropout & Re-enrolment** – trends in dropout and re-enrolment behavior

### How to open
1. Install [Power BI Desktop](https://powerbi.microsoft.com/desktop/) (Windows only).
2. Open `DashBoard.pbix`.
3. If prompted, point the data source connections to the CSVs in `Data/Gold/` (or refresh from your own Databricks connection).

---

## ▶️ How to Reproduce the Pipeline

1. Upload the raw source files (`learners.csv`, `courses.csv`, `enrolment_activity.csv`) to your Databricks Volume path (e.g. `/Volumes/learntrack_lms_analytics/default/bronze_layer`).
2. Run `NoteBooks/Bronze_Layer.py` to ingest raw data into Bronze tables.
3. Run `NoteBooks/Silver_Layer.py` to clean, deduplicate, and join data into Silver tables.
4. Run `NoteBooks/Data_Validation.py` to validate row counts, duplicates, and primary keys.
5. Run `NoteBooks/Gold_Layer.py` to build the Gold business tables.
6. Run queries in `NoteBooks/sql_analysis.sql` for ad-hoc analysis, or export the Gold tables to CSV.
7. Open `DashBoard.pbix` in Power BI Desktop and refresh the data connections to visualize the results.

See `WorkFlow/01_pipeline.png`, `02_running.png`, and `03_completed.png` for a visual walkthrough of the Databricks job pipeline execution.

---

## 📈 Sample Gold Table Schemas

**`gold_course_completion.csv`**
`course_id, course_title, category, total_enrolments, completed_learners, completion_rate, completion_category`

**`gold_assessment_performance.csv`**
`course_id, course_title, average_score, passed, attempted, pass_rate`

**`gold_instructor_performance.csv`**
`instructor_id, instructor_name, avg_score, avg_rating, total_enrolments, completed, completion_rate, rank`

**`gold_learner_engagement.csv`**
`course_id, learner_id, enrolment_id, ..., days_since_last_activity, engagement_status`

**`gold_dropout_reenrolment.csv`**
`course_id, learner_id, enrolment_id, ..., completion_delay_days, ...`

---

## 📝 Notes

- This project was originally developed as a Databricks + Power BI capstone/final project.
- Update the Unity Catalog paths (`learntrack_lms_analytics.default.*`) in the notebooks if running under a different workspace/catalog name.
