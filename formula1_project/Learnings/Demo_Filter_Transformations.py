# Databricks notebook source
# MAGIC %run "../includes/Configuration"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/Common_functions"

# COMMAND ----------

races_df= spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(races_df)

# COMMAND ----------

#races_filtered_df = races_df.filter("race_year = 2019") ###Same as SQL , even we can use where in place of filter
from pyspark.sql.functions import col
#races_filtered_df = races_df.filter(col("race_year") == 2019)
#races_filter = races_df.filter(races_df.race_year == 2019)
races_filter = races_df.filter(races_df['race_year'] == 2019)

# COMMAND ----------

display(races_filter)

# COMMAND ----------

dbutils.widgets.text("keyname","testng","source")
value = dbutils.widgets.get("keyname")

# COMMAND ----------

value

# COMMAND ----------

races_filtered_df1 = races_df.filter("race_year = 2019 and round <= 5") ##SQL Way
races_filtered_df2 = races_df.filter((col("race_year")== 2019) & (col("round")<=5)) ##Pyspark Way


# COMMAND ----------

display(races_filtered_df2)

# COMMAND ----------


