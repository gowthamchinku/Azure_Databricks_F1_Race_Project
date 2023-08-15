# Databricks notebook source
# MAGIC %md
# MAGIC #Explore the DBFS Root
# MAGIC 1. List the folders in the DBFS Root
# MAGIC 2. Interact with the DBFS File Browser
# MAGIC 3. Upload file to DBFS Root

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

display(dbutils.fs.ls("/"))
display(dbutils.fs.ls("dbfs:/FileStore/"))

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv', header = True))
