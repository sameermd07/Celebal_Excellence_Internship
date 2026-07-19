# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer

# COMMAND ----------

silver_df = spark.table("learntrack_lms_analytics.default.silver_lms")

silver_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 1 — Course Completion Rate
# MAGIC Business Question
# MAGIC
# MAGIC What percentage of learners completed each course?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Aggregate

# COMMAND ----------

from pyspark.sql.functions import *

course_completion = (
    silver_df
    .groupBy(
        "course_id",
        "course_title",
        "category"
    )
    .agg(
        count("*").alias("total_enrolments"),

        sum(
            when(col("status") == "Completed", 1).otherwise(0)
        ).alias("completed_learners")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Calculate Completion %

# COMMAND ----------

course_completion = course_completion.withColumn(
    "completion_rate",
    round(
        col("completed_learners") * 100 / col("total_enrolments"),
        2
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Classify Courses

# COMMAND ----------

course_completion = course_completion.withColumn(
    "completion_category",
    when(col("completion_rate") >= 80, "High Completion")
    .when(col("completion_rate") >= 50, "Moderate")
    .otherwise("At Risk")
)

# COMMAND ----------

course_completion.show(10)

# COMMAND ----------

course_completion.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("learntrack_lms_analytics.default.gold_course_completion")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 2 — Learner Engagement
# MAGIC Business Question
# MAGIC
# MAGIC Which learners are active and which are inactive?

# COMMAND ----------

learner_engagement = silver_df.withColumn(
    "days_since_last_activity",
    datediff(current_date(), col("last_activity_date"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Engagement Status

# COMMAND ----------

learner_engagement = learner_engagement.withColumn(
    "engagement_status",
    when(col("days_since_last_activity") <= 7, "Highly Active")
    .when(col("days_since_last_activity") <= 30, "Active")
    .otherwise("Disengaged")
)

# COMMAND ----------

learner_engagement.show(10)

# COMMAND ----------

learner_engagement.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("learntrack_lms_analytics.default.gold_learner_engagement")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 3 — Instructor Performance
# MAGIC Business Question
# MAGIC
# MAGIC Which instructors perform best?

# COMMAND ----------

instructor_performance = (
    silver_df
    .groupBy(
        "instructor_id",
        "instructor_name"
    )
    .agg(

        avg("assessment_score").alias("avg_score"),

        avg("feedback_rating").alias("avg_rating"),

        count("*").alias("total_enrolments"),

        sum(
            when(col("status")=="Completed",1).otherwise(0)
        ).alias("completed")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completion Rate

# COMMAND ----------

instructor_performance = instructor_performance.withColumn(
    "completion_rate",
    round(
        col("completed")*100/col("total_enrolments"),
        2
    )
)

# COMMAND ----------

instructor_performance.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rank Instructors

# COMMAND ----------

from pyspark.sql.window import Window

windowSpec = Window.orderBy(desc("completion_rate"))

instructor_performance = instructor_performance.withColumn(
    "rank",
    dense_rank().over(windowSpec)
)

# COMMAND ----------

instructor_performance.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("learntrack_lms_analytics.default.gold_instructor_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 4 — Assessment Performance
# MAGIC Business Question
# MAGIC
# MAGIC Which assessments are difficult?

# COMMAND ----------

assessment_performance = (
    silver_df
    .groupBy(
        "course_id",
        "course_title"
    )
    .agg(

        avg("assessment_score").alias("average_score"),

        max("assessment_score").alias("highest_score"),

        min("assessment_score").alias("lowest_score"),

        count("assessment_score").alias("attempts")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Pass Rate
# MAGIC
# MAGIC Let's assume 40 marks is the passing score.

# COMMAND ----------

assessment_performance = (
    silver_df
    .groupBy(
        "course_id",
        "course_title"
    )
    .agg(

        avg("assessment_score").alias("average_score"),

        sum(
            when(col("assessment_score") >= 40,1)
            .otherwise(0)
        ).alias("passed"),

        count("assessment_score").alias("attempted")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Pass %

# COMMAND ----------

assessment_performance = assessment_performance.withColumn(
    "pass_rate",
    round(
        col("passed")*100/col("attempted"),
        2
    )
)

# COMMAND ----------

assessment_performance.show(10)

# COMMAND ----------

assessment_performance.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("learntrack_lms_analytics.default.gold_assessment_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 5 — Dropout & Re-enrolment
# MAGIC Business Question
# MAGIC
# MAGIC Who dropped and enrolled again?

# COMMAND ----------

reenrolment = silver_df.filter(
    col("attempts") >= 2
)

# COMMAND ----------

from pyspark.sql.window import Window

windowSpec = Window.partitionBy(
    "learner_id",
    "course_id"
).orderBy(desc("enrol_date"))

# COMMAND ----------

latest_attempt = silver_df.withColumn(
    "row_num",
    row_number().over(windowSpec)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Keep latest

# COMMAND ----------

latest_attempt = latest_attempt.filter(
    col("row_num")==1
)

# COMMAND ----------

latest_attempt.show(10)

# COMMAND ----------

latest_attempt.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("learntrack_lms_analytics.default.gold_dropout_reenrolment")

# COMMAND ----------

# MAGIC %md
# MAGIC | Table                         | Purpose                             |
# MAGIC | ----------------------------- | ----------------------------------- |
# MAGIC | `gold_course_completion`      | Course-wise completion rates        |
# MAGIC | `gold_learner_engagement`     | Active vs disengaged learners       |
# MAGIC | `gold_instructor_performance` | Instructor rankings and metrics     |
# MAGIC | `gold_assessment_performance` | Assessment pass rates and scores    |
# MAGIC | `gold_dropout_reenrolment`    | Latest enrolments and re-enrolments |
# MAGIC