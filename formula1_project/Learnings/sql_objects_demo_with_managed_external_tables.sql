-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Objectives of Notebook
-- MAGIC 1. Spark SQL Documentation
-- MAGIC 2. Create Database demo
-- MAGIC 3. Data Tab in UI
-- MAGIC 4. SHOW and Describe command
-- MAGIC 5. Find current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE DEMO;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED DEMO;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

USE DEMO;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN DEMO;

-- COMMAND ----------

SHOW TABLES IN default;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Managed Tables

-- COMMAND ----------

-- MAGIC %run "../includes/Configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")        

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").mode("overwrite").saveAsTable("demo.race_results_python")

-- COMMAND ----------

 USE DEMO

-- COMMAND ----------

SHOW TABLES IN DEMO 

-- COMMAND ----------

DESC EXTENDED race_results_python

-- COMMAND ----------

SELECT * 
FROM demo.race_results_python
WHERE race_year =2020;

-- COMMAND ----------

-- Create Managed Tables in SQL

-- COMMAND ----------

CREATE TABLE race_results_sql
AS
SELECT * 
FROM race_results_python
WHERE race_year = 2020

-- COMMAND ----------

SELECT *
FROM demo.race_results_sql; 

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES IN DEMO

-- COMMAND ----------

DESCRIBE EXTENDED DEMO.race_results_sql;


-- COMMAND ----------

drop table demo.race_results_sql;
-- dropping the managed table here, will drop the metadata of the table and the data because they have a hardlink and tightly coupled.

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### External Tables

-- COMMAND ----------

-- Create external tables using Python
-- Create external tables using SQl
-- Effect of dropping an external Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").mode("overwrite").option("path",f"{presentation_folder_path}/ext_race_results").saveAsTable("demo.race_res_python")

-- COMMAND ----------

SHOW DATABASES;


-- COMMAND ----------

SHOW TABLES IN DEMO

-- COMMAND ----------

DESCRIBE EXTENDED race_res_python;

-- COMMAND ----------

select * 
FROM demo.race_res_python;

-- COMMAND ----------

CREATE
 TABLE demo.race_results_sql1
(
race_year INT,
race_name STRING,
race_datae TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number STRING,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USINg PARQUET
LOCATION "/mnt/formula1dlgo/presentation/race_results_sql1"

-- COMMAND ----------

-- Insert can be done using multi row insertion and single row insertion and select from a diff table

INSERT INTO demo.race_results_sql
SELECT * 
FROM demo.race_res_python WHERE race_year = 2020

-- COMMAND ----------

drop table demo.race_results_sql;

-- COMMAND ----------


