# Databricks notebook source
# MAGIC %md
# MAGIC ###  Ingest Circuits.csv File
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1 : Read the csv file using spark dataframe reader(dataframe source API)

# COMMAND ----------

circuits_df = spark.read.option("header",True).csv("dbfs:/mnt/formula1dlgo/raw/circuits.csv")
#circuits_df = spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/mnt/formula1dlgo/raw/circuits.csv")

# COMMAND ----------

#type(circuits_df)
#circuits_df.show(30, truncate = False)
display(circuits_df)

# display(circuitsdf)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### - Lets us focus on the Schema

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## More about Schema we provide to the Spark
# MAGIC
# MAGIC - As the inferSchema is really not good idea during the production, because it will take huge amount of time to scan the whole data
# MAGIC - So we actually provide the schema to the spark
# MAGIC - In order to provide the schema to the spark we should be aware of datatypes
# MAGIC - Among the StructType (As a complete row) and StructFields(about each column) is important

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), False),
                                     StructField("name", StringType(), False),
                                     StructField("location", StringType(), False),
                                     StructField("country", StringType(), False),
                                     StructField("lat", DoubleType(), False),
                                     StructField("lng", DoubleType(), False),
                                     StructField("alt", IntegerType(), False),
                                     StructField("url", StringType(), False),
                                     ])

# COMMAND ----------

circuits_df1 = spark.read \
    .option("header",True) \
    .schema(circuits_schema) \
    .csv("dbfs:/mnt/formula1dlgo/raw/circuits.csv")


# COMMAND ----------

#circuits_df1.show()
circuits_df1.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### - This is about file system magic commands . calling dbutils.fs.mounts() is same as using magic commands 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlgo/raw
