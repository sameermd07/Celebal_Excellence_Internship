# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Bronze Tables

# COMMAND ----------

bronze_learners_df = spark.table("learntrack_lms_analytics.default.bronze_learners")

bronze_courses_df = spark.table("learntrack_lms_analytics.default.bronze_courses")

bronze_enrolment_df = spark.table("learntrack_lms_analytics.default.bronze_enrolment_activity")

# COMMAND ----------

(bronze_learners_df).show()


# COMMAND ----------


(bronze_courses_df).show()



# COMMAND ----------

(bronze_enrolment_df).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Remove Duplicate Enrolments

# COMMAND ----------

bronze_enrolment_df.count()

# COMMAND ----------

silver_enrolment_df = bronze_enrolment_df.dropDuplicates(["enrolment_id"])

# COMMAND ----------

print("Before :", bronze_enrolment_df.count())


# COMMAND ----------

print("After  :", silver_enrolment_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Check Missing Values

# COMMAND ----------

from pyspark.sql.functions import col, sum, when

display(silver_enrolment_df.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in silver_enrolment_df.columns
]))

# COMMAND ----------

bronze_courses_df.select([
    sum(when(col(c).isNull(),1).otherwise(0)).alias(c)
    for c in bronze_courses_df.columns
]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Handle Missing instructor_name

# COMMAND ----------

bronze_courses_df.filter(
    col("instructor_name").isNull()
).show()

# COMMAND ----------

silver_courses_df = bronze_courses_df.fillna({
    "instructor_name": "Unknown"
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Convert Date Columns

# COMMAND ----------

from pyspark.sql.functions import to_date
silver_learners_df = bronze_learners_df.withColumn(
    "registration_date",
    to_date("registration_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enrolment dates:

# COMMAND ----------

silver_enrolment_df = silver_enrolment_df \
.withColumn("enrol_date", to_date("enrol_date")) \
.withColumn("expected_completion_date", to_date("expected_completion_date")) \
.withColumn("actual_completion_date", to_date("actual_completion_date")) \
.withColumn("last_activity_date", to_date("last_activity_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Derived Columns

# COMMAND ----------

from pyspark.sql.functions import datediff
silver_enrolment_df = silver_enrolment_df.withColumn(
    "learning_duration_days",
    datediff(
        col("actual_completion_date"),
        col("enrol_date")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completion Delay

# COMMAND ----------

silver_enrolment_df = silver_enrolment_df.withColumn(
    "completion_delay_days",
    datediff(
        col("actual_completion_date"),
        col("expected_completion_date")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Join the Tables

# COMMAND ----------

silver_df = (
    silver_enrolment_df.alias("e")
    .join(
        silver_learners_df.alias("l"),
        on="learner_id",
        how="left"
    )
    .join(
        silver_courses_df.alias("c"),
        on="course_id",
        how="left"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Verify

# COMMAND ----------

silver_df.show(10)

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

silver_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Save Silver Tables

# COMMAND ----------

silver_learners_df.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("learntrack_lms_analytics.default.silver_learners")

# COMMAND ----------

silver_courses_df.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("learntrack_lms_analytics.default.silver_courses")

# COMMAND ----------

silver_enrolment_df.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("learntrack_lms_analytics.default.silver_enrolment_activity")

# COMMAND ----------

silver_df.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("learntrack_lms_analytics.default.silver_lms")