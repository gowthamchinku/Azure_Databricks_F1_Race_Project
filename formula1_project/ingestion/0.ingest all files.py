# Databricks notebook source
# MAGIC %md
# MAGIC ### This notebook is to show the workflows utility provided through notebooks as part of dbutils

# COMMAND ----------

result1 = dbutils.notebook.run("1.ingest_circuit_files",0,{"test_widget":"Ergast APII"})

# COMMAND ----------

result1

# COMMAND ----------

result2 = dbutils.notebook.run("2.ingesting_the_races_file",0,{"Source":"Ergast APII"})
result2

# COMMAND ----------

result3 = dbutils.notebook.run("3.ingest_constructor_file",0,{"Source":"Ergast APII"})
result3

# COMMAND ----------

result4 = dbutils.notebook.run("4. Ingest_Drivers_Data",0,{"Source":"Ergast APII"})
result4

# COMMAND ----------

result5 = dbutils.notebook.run("5.Ingest_Results_File",0,{"Source":"Ergast APII"})
result5

# COMMAND ----------

result6 = dbutils.notebook.run("6. Ingest_Pits_Stops_File",0,{"Source":"Ergast APII"})
result6

# COMMAND ----------

result7 = dbutils.notebook.run("7. Data_Ingestion_Lap_Times_Multiple.csv",0,{"Source":"Ergast APII"})
result7

# COMMAND ----------

result8 = dbutils.notebook.run("8. Ingest_Qualfying_Data",0,{"Source":"Ergast APII"})
result8

# COMMAND ----------

dbutils.help()

# COMMAND ----------


