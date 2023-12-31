# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest the constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC -- Step 1 : Read the Json file using the spark dataframe reader

# COMMAND ----------

# MAGIC %run "../includes/Configuration"

# COMMAND ----------

# MAGIC %run "../includes/Common_functions"

# COMMAND ----------

dbutils.widgets.text("Source","","Source")
source=dbutils.widgets.get("Source")


# COMMAND ----------

# Using DDL type to define the schema

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructor_schema).format("json")\
.load(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC -- Drop the unwanted_columns

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,lit

# COMMAND ----------

constructor_dropped_df1 = constructor_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### rename columns and add ingested column

# COMMAND ----------

constructor_final_df = constructor_dropped_df1.withColumnRenamed("constructorId","constructor_id") \
                      .withColumnRenamed("constructorRef","constructor_ref")\
                      .withColumn("ingestion_date",current_timestamp())\
                      .withColumn("Source",lit(source))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

#write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").save(f"{processed_folder_path}/constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/formula1dlgo/processed/constructors

# COMMAND ----------

dbutils.notebook.exit("Success")
