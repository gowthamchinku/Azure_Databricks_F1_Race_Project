# Databricks notebook source
# MAGIC %run "../includes/Configuration"

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits1")\
  .withColumnRenamed("name","ciruits_name")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter(col("race_year")==2019)\
  .withColumnRenamed("name","race_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

## In how by default it is inner join even we don't mention about the type of join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_Id == races_df.circuit_id,"inner")\
  .select(circuits_df.ciruits_name, circuits_df.location,circuits_df.race_country,races_df.race_name,races_df.round)


# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df.select("ciruits_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left Outer Join

# COMMAND ----------

circuits_df_left = spark.read.parquet(f"{processed_folder_path}/circuits1")\
  .withColumnRenamed("name","ciruits_name")\
  .filter(col("circuit_id")<70)

# COMMAND ----------


