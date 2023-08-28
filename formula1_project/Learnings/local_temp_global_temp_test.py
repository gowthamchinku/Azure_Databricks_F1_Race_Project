# Databricks notebook source
race_19 =spark.sql("SELECT * FROM View_Name WHERE race_year = 2019")

# COMMAND ----------

df=spark.sql("SELECT * FROM global_temp.global_view")

# COMMAND ----------

df.show()

# COMMAND ----------


