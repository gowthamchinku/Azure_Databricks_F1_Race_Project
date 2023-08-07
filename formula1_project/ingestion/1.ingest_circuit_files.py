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
# MAGIC - Select only requiredColumns

# COMMAND ----------

## 1st way to selecting the dataframe 
circuits_selected_df1 = circuits_df1.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

display(circuits_selected_df1)

# COMMAND ----------

# 2nd way to selecting the dataframe 
circuits_selected_df2 = circuits_df1.select(circuits_df1.circuitId,circuits_df1.circuitRef,circuits_df1.name,circuits_df1.location,circuits_df1.country,circuits_df1.lat,circuits_df1.lng,circuits_df1.alt)

# COMMAND ----------

display(circuits_selected_df2)

# COMMAND ----------

# 3rd way of selecting columns from a dataframe
circuits_selected_df3 = circuits_df1.select(circuits_df1["circuitId"],circuits_df1["circuitRef"],circuits_df1["name"],circuits_df1["location"],circuits_df1["country"],circuits_df1["lat"],circuits_df1["lng"],circuits_df1["alt"])

# COMMAND ----------

circuits_selected_df3.show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

#using the 4th option which is nothing but with col
circuits_selected_df4 = circuits_df1.select(col("circuitId") \
    ,col("circuitRef"),col("name"),col("location"),col("country").alias("race_country")\
    ,col("lat"),col("lng"),col("alt"))

# COMMAND ----------

circuits_selected_df4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Rename the columns can be done in couple of ways
# MAGIC 1) using an alias as showed above
# MAGIC 2) using a specific api withColumnRenamed

# COMMAND ----------

circuits_df_renamed = circuits_selected_df4.withColumnRenamed("circuitId","circuit_Id")\
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lng","longitude")\
    .withColumnRenamed("alt","altitude")

# COMMAND ----------

display(circuits_df_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding the new columns 
# MAGIC - Can be achieved through withColumn
# MAGIC - Here i am adding an audit columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
circuits_final_df = circuits_df_renamed.withColumn("ingested_date",current_timestamp())

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC - Use lit function let us say if we want a value as a column with the string/ value   (Just to know about the lit)

# COMMAND ----------

from pyspark.sql.functions import lit

circuits_final_df.withColumn("Env",lit("Production")).show()

# COMMAND ----------


