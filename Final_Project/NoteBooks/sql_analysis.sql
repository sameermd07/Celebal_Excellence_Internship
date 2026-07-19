-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SQL Analytics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 1
-- MAGIC ## 
-- MAGIC Top 10 courses by completion rate.

-- COMMAND ----------

SELECT *
FROM learntrack_lms_analytics.default.gold_course_completion
ORDER BY completion_rate DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query 2 — At Risk Courses

-- COMMAND ----------


SELECT
    course_title,
    completion_rate,
    completion_category
FROM learntrack_lms_analytics.default.gold_course_completion
WHERE completion_category = 'At Risk' limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query 3 — Best Instructors

-- COMMAND ----------


SELECT
    instructor_name,
    completion_rate,
    avg_score,
    rank
FROM learntrack_lms_analytics.default.gold_instructor_performance
ORDER BY rank;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##  Query 4 — Disengaged Learners

-- COMMAND ----------


SELECT
    learner_name,
    course_title,
    days_since_last_activity
FROM learntrack_lms_analytics.default.gold_learner_engagement
WHERE engagement_status='Disengaged' limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query 5 — Difficult Courses

-- COMMAND ----------


SELECT
    course_title,
    average_score,
    pass_rate
FROM learntrack_lms_analytics.default.gold_assessment_performance
ORDER BY pass_rate limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query 6 — Re-enrolled Learners

-- COMMAND ----------


SELECT
    learner_name,
    course_title,
    attempts
FROM learntrack_lms_analytics.default.gold_dropout_reenrolment
WHERE attempts>=2 limit 10;