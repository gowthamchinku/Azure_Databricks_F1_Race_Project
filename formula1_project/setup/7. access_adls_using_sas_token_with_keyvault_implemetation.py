# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake using SAS Token
# MAGIC #### Steps to achieve it
# MAGIC
# MAGIC 1. Set the spark config for SAS token
# MAGIC 2. List all the files in the container using dbutils
# MAGIC 3. Read the data using spark reader api

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

#dbutils.secrets.help()
dbutils.secrets.listScopes()

# COMMAND ----------

#dbutils.secrets.help()
#dbutils.secrets.listScopes()
dbutils.secrets.list(scope = "formula1sastoken")


# COMMAND ----------

#dbutils.secrets.list(scope = 'Formula1-SAS-Token_ADLS_Container')
sas_secret_key = dbutils.secrets.get(scope = "formula1sastoken", key = 'formula1dl-sastoken')
#print(sas_secret_key)  # here the sas key is redacted which means we can't access the keys

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlgo.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlgo.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlgo.dfs.core.windows.net", sas_secret_key)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlgo.dfs.core.windows.net") 

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlgo.dfs.core.windows.net") )

# COMMAND ----------

#display(spark.read.csv("abfss://demo@formula1dlgo.dfs.core.windows.net/circuits.csv",header= True))

# COMMAND ----------

""" df1 = spark.read.format.option("header", True).load("abfss://demo@formula1dlgo.dfs.core.windows.net/circuits.csv")
 df1.show(50)
"""
spark
