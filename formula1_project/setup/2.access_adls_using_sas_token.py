# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake using SAS Token
# MAGIC #### Steps to achieve it
# MAGIC
# MAGIC 1. Set the spark config for SAS token
# MAGIC 2. List all the files in the container using dbutils
# MAGIC 3. Read the data using spark reader api

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlgo.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlgo.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlgo.dfs.core.windows.net", "sv=2021-06-08&st=2023-07-30T22%3A14%3A55Z&se=2023-07-31T22%3A14%3A55Z&sr=c&sp=rl&sig=rhC0WQ8nkAFikUHL8cuDpc4lko1OUpYFmcBjqSuARls%3D")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlgo.dfs.core.windows.net") 

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlgo.dfs.core.windows.net") )

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlgo.dfs.core.windows.net/circuits.csv",header= True))

# COMMAND ----------

 
