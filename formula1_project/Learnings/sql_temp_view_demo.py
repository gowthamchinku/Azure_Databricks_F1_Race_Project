# Databricks notebook source
# MAGIC %md
# MAGIC ### Access dataframes using SQL
# MAGIC - Objectives
# MAGIC 1. Create Temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python Cell

# COMMAND ----------

# MAGIC %run "../includes/Configuration"

# COMMAND ----------

races_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

#races_results_df.createTempView("View_Name")  # This will create a temp view on dataframe. But the problem is it will throw the error once again\
  # if we run the same command because the view is already created. So it is better to use createOrReplaceTempView
races_results_df.createOrReplaceTempView("View_Name")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) 
# MAGIC FROM View_Name
# MAGIC WHERE race_year =2020

# COMMAND ----------

# How to run sql with Pyspark, Spark gives the method sql
race_19 =spark.sql("SELECT * FROM View_Name WHERE race_year = 2019")

# COMMAND ----------

display(race_19)

# COMMAND ----------

p_race = 2019
race_19_p_df=spark.sql(f"SELECT * FROM View_Name WHERE race_year = {p_race}")

# COMMAND ----------

display(race_19_p_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Global Temp Views
# MAGIC 1. Create global temporary views on dataframes
# MAGIC 2. Access the view from SQL code
# MAGIC 3. Access the view from python cell
# MAGIC 4. Access the view from another notebook

# COMMAND ----------

races_results_df.createOrReplaceGlobalTempView("global_view")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from global_temp.global_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES IN GLOBAL_TEMP;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM global_temp.global_view;

# COMMAND ----------

df=spark.sql("SELECT * FROM global_temp.global_view")

# COMMAND ----------

df.show()

# COMMAND ----------


