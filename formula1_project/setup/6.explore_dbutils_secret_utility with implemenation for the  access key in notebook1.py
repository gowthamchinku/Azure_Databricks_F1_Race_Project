# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Exploring the capabailities of dbutils.secrets utility with implemenatation

# COMMAND ----------

formula1dl_accountkey = dbutils.secrets.get(scope ='formula1-Scope', key ='Formula1dl-accountkey')

# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1dlgo.dfs.core.windows.net",formula1dl_accountkey)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlgo.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlgo.dfs.core.windows.net") )

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlgo.dfs.core.windows.net/circuits.csv", header = True))

# COMMAND ----------


