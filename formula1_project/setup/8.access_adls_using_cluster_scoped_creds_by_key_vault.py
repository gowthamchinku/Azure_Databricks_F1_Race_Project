# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake using cluster scoped creds
# MAGIC #### Steps to achieve it
# MAGIC
# MAGIC 1. Set the spark config with the fs.azure.account.key in the cluster scope
# MAGIC 2. List all the files in the container using dbutils
# MAGIC 3. Read the data using spark reader api

# COMMAND ----------

#this is now not required because we are passing the values in the cluster scope in the advanced settings 
#spark.conf.set("fs.azure.account.key.formula1dlgo.dfs.core.windows.net","o6cxUThFPf3cZuVpgXC6vOvwzkWgebH5QpzKZOmIveXxwlUBCFt9VbiYoVFMgV3s4myrdgBnEWGz+AStuDQ6bQ==")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlgo.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlgo.dfs.core.windows.net") )

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlgo.dfs.core.windows.net/circuits.csv", header= True))

# COMMAND ----------

 
