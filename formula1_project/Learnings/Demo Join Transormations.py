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

display(circuits_df_left)

# COMMAND ----------

race_circuits_left = circuits_df_left.join(races_df,circuits_df_left.circuit_Id==races_df.circuit_id,"left")

# COMMAND ----------

display(race_circuits_left)

# COMMAND ----------

race_circuits_right= circuits_df_left.join(races_df,circuits_df_left.circuit_Id==races_df.circuit_id,"right")

# COMMAND ----------

display(race_circuits_right)

# COMMAND ----------

race_circuits_full= circuits_df_left.join(races_df,circuits_df_left.circuit_Id==races_df.circuit_id,"full")

# COMMAND ----------

display(race_circuits_full)

# COMMAND ----------

values1 = [(1,), (2,), (3,), (4,), (10,), (12,), (15,), (16,),(1,)]
schema = "id INT"

# Create a DataFrame
df1 = spark.createDataFrame(values1, schema)

# COMMAND ----------

df1.show()

# COMMAND ----------

values2 = [(1,), (2,), (3,), (8,), (1,), (1,), (4,), (2,),(12,)]
schema = "id INT"

# Create a DataFrame
df2 = spark.createDataFrame(values2, schema)

# COMMAND ----------

df2.show()

# COMMAND ----------

inner_j = df1.join(df2,df1.id == df2.id, "inner")
inner_j.show()

# COMMAND ----------

left_j = df1.join(df2,df1.id == df2.id, "left")
left_j.show()

# COMMAND ----------

right_j = df1.join(df2,df1.id == df2.id, "right")
right_j.show()

# COMMAND ----------

from pyspark.sql.functions import col
outer_j = df1.join(df2,df1.id == df2.id, "outer")
outer_j.show()

# COMMAND ----------


