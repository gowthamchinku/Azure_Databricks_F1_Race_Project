# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting the divers data with nested json
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/Configuration"

# COMMAND ----------

# MAGIC %run "../includes/Common_functions"

# COMMAND ----------

dbutils.widgets.text("Source","","Source")
source=dbutils.widgets.get("Source")


# COMMAND ----------

# MAGIC %md
# MAGIC -- Step 1: Read the JSON File using spark dataframe reader API
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC -- Here the schema import is different we have nested json with struct type

# COMMAND ----------

# for inner_json_object

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)])

# COMMAND ----------

# Here for the name inner json, we can give the type as the json_defined

drivers_schema = StructType(fields = [StructField("driverId",IntegerType(), False),
                            StructField("driverRef",StringType(), True),
                            StructField("dob",StringType(), True),
                            StructField("code",StringType(), True),
                            StructField("name",name_schema, True),
                            StructField("nationality",StringType(), True),
                            StructField("number",StringType(), True),
                            StructField("url",StringType(), True),
                            ])

# COMMAND ----------

drivers_df = spark.read.format("json").schema(drivers_schema).load(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2
# MAGIC - Rename the columns
# MAGIC - Ingestion date should be added
# MAGIC - name added with concatenation of the forename and the surname

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,concat,lit,col

# COMMAND ----------

driver_modified_df = drivers_df.withColumnRenamed("driverId","driver_id")\
                    .withColumnRenamed("driverRef","driver_ref")\
                    .withColumn("ingested_date",current_timestamp())\
                    .withColumn("name",concat(col("name.forename"), lit(" "),col("name.surname")))\
                    .withColumn("Source",lit(source))

# COMMAND ----------

driver_modified_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3:
# MAGIC

# COMMAND ----------

# Drop the url

drivers_final = driver_modified_df.drop("url")

# COMMAND ----------

drivers_final.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to parquet
# MAGIC
# MAGIC

# COMMAND ----------

drivers_final.write.format("parquet").mode("overwrite").save(f"{processed_folder_path}/driver")

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/formula1dlgo/processed/driver

# COMMAND ----------

dbutils.notebook.exit("Success")
