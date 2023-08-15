# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Exploring the capabailities of dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='formula1-Scope')

# COMMAND ----------

dbutils.secrets.get(scope ='formula1-Scope', key ='Formula1dl-accountkey')
