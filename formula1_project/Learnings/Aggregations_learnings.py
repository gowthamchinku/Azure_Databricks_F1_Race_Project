# Databricks notebook source
# MAGIC %md
# MAGIC #### Mainly we have three types of aggregations
# MAGIC - Simple Aggregations like sum, min, max, count
# MAGIC - Grouped Aggregations
# MAGIC - Window Aggregations

# COMMAND ----------

# MAGIC %run "../includes/Configuration"

# COMMAND ----------

race_results_df  = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df = race_results_df.filter(race_results_df.race_year==2020)

# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct, count

# COMMAND ----------

demo_df.select(count('*')).show()

# COMMAND ----------

demo_df.select(count('race_name')).show()

# COMMAND ----------

demo_df.select(countDistinct('race_name')).show()

# COMMAND ----------

demo_df.select(sum('points')).show()

# COMMAND ----------

demo_df.filter(demo_df.driver_name=='Lewis Hamilton').select(sum('points'), countDistinct("race_name"))\
  .withColumnRenamed("sum(points)","total_points")\
  .withColumnRenamed("count(DISTINCT race_name)","number_of_races")\
  .show()


# COMMAND ----------


