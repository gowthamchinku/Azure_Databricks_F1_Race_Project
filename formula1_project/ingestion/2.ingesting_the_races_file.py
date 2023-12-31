# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting the races file

# COMMAND ----------

# MAGIC %run "../includes/Configuration"

# COMMAND ----------

# MAGIC %run "../includes/Common_functions"

# COMMAND ----------

dbutils.widgets.text("Source","","Source")
source=dbutils.widgets.get("Source")


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlgo/raw/
# MAGIC

# COMMAND ----------

from pyspark.sql.types import *
race_schema = StructType(fields = [StructField("raceId",IntegerType(), False),
                                   StructField("year",IntegerType(), True),
                                   StructField("round",IntegerType(), True),
                                   StructField("circuitId",IntegerType(), True),
                                   StructField("name",StringType(), True),
                                   StructField("date",DateType(), True),
                                   StructField("time",StringType(), True),
                                   StructField("url",IntegerType(), True)
                                   ])

# COMMAND ----------

## Read the file using dataframe reader API

racesdf = spark.read.format("csv").schema(race_schema).option("header",True).load(f"{raw_folder_path}/races.csv")
racesdf.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,concat,col,to_timestamp,lit
#Transformation - Dropping the url
final_races_data = racesdf.drop("url").withColumnRenamed("raceId","race_id")\
.withColumnRenamed("year","race_year") \
.withColumnRenamed("circuitId","circuit_id") \
.withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),'yyyy-MM-dd HH:mm:ss')) \
.withColumn("ingestion_date", current_timestamp()).drop("date").drop("time")\
.withColumn("Source",lit(source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write this data to parquet file
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

final_races_data.write.format("parquet").partitionBy('race_year').mode("overwrite").save(f"{processed_folder_path }/races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlgo/processed/races

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlgo/processed/races"))

# COMMAND ----------

dbutils.notebook.exit("Success")
