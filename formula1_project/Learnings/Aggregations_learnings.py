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

# MAGIC %md
# MAGIC
# MAGIC ### Grouped Aggregations
# MAGIC

# COMMAND ----------

demo_df\
  .groupBy("driver_name")\
  .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")).show()
 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Windowing

# COMMAND ----------

demo_df = race_results_df.filter("race_year in (2019,2020)")
display(demo_df)
demo_grouped_df = demo_df\
  .groupBy("race_year","driver_name")\
  .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))
 


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank

# COMMAND ----------

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))

demo_grouped_df.withColumn("Rank", rank().over(driverRankSpec)).show(100)

# COMMAND ----------


