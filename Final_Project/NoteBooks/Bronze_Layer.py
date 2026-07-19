# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Import Required Libraries

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Define File Paths

# COMMAND ----------

bronze_path = "/Volumes/learntrack_lms_analytics/default/bronze_layer"

learners_file = f"{bronze_path}/learners.csv"
courses_file = f"{bronze_path}/courses.csv"
enrolment_file = f"{bronze_path}/enrolment_activity.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Read the CSV Files

# COMMAND ----------

learners_df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv(learners_file)
)

courses_df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv(courses_file)
)

enrolment_df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv(enrolment_file)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify the Data

# COMMAND ----------

learners_df.show(5, truncate=False)


# COMMAND ----------

courses_df.show(5, truncate=False)


# COMMAND ----------

enrolment_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Check the Schema

# COMMAND ----------

learners_df.printSchema()



# COMMAND ----------

courses_df.printSchema()


# COMMAND ----------


enrolment_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Count the Records

# COMMAND ----------

print("Learners :", learners_df.count())


# COMMAND ----------

print("Courses :", courses_df.count())


# COMMAND ----------

print("Enrolments :", enrolment_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Write Bronze Tables as Delta

# COMMAND ----------

learners_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("learntrack_lms_analytics.default.bronze_learners")

# COMMAND ----------

courses_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("learntrack_lms_analytics.default.bronze_courses")

# COMMAND ----------

enrolment_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("learntrack_lms_analytics.default.bronze_enrolment_activity")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Verify the Bronze Tables

# COMMAND ----------

(spark.table("learntrack_lms_analytics.default.bronze_learners")).show(10)

# COMMAND ----------

(spark.table("learntrack_lms_analytics.default.bronze_courses")).show()


# COMMAND ----------


(spark.table("learntrack_lms_analytics.default.bronze_enrolment_activity")).show()