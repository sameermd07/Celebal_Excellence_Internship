# Databricks notebook source
# MAGIC %md
# MAGIC # Data Validation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation 1: Record Count Validation

# COMMAND ----------

bronze = spark.table("learntrack_lms_analytics.default.bronze_enrolment_activity")

silver = spark.table("learntrack_lms_analytics.default.silver_enrolment_activity")

print(f"Bronze Records : {bronze.count()}")
print(f"Silver Records : {silver.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation 2: Duplicate Check

# COMMAND ----------

duplicates = (
    silver
    .groupBy("enrolment_id")
    .count()
    .filter("count > 1")
)

display(duplicates)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation 3: Primary Key Check

# COMMAND ----------

from pyspark.sql.functions import col

silver.filter(
    col("enrolment_id").isNull()
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation 4: Status Validation

# COMMAND ----------

silver.select("status").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation 5: Progress Percentage

# COMMAND ----------

silver.filter(
    (col("progress_pct") < 0) |
    (col("progress_pct") > 100)
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation 6: Assessment Score
# MAGIC

# COMMAND ----------

silver.filter(
    (col("assessment_score") < 0) |
    (col("assessment_score") > 100)
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation 7: Certificate Issued

# COMMAND ----------

silver.groupBy("certificate_issued").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation 8: Completion Date Logic

# COMMAND ----------

silver.filter(
    (col("status") == "Completed") &
    (col("actual_completion_date").isNull())
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation 9: Gold Table Validation

# COMMAND ----------

gold_tables = [
    "gold_course_completion",
    "gold_learner_engagement",
    "gold_instructor_performance",
    "gold_assessment_performance",
    "gold_dropout_reenrolment"
]

for table in gold_tables:
    df = spark.table(f"learntrack_lms_analytics.default.{table}")
    print(f"{table}: {df.count()} rows")

# COMMAND ----------

