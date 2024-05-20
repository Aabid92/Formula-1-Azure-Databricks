-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Lesson Objectives
-- MAGIC - Spark SQL Documentation
-- MAGIC - Create Database demo
-- MAGIC - Data tab in the UI
-- MAGIC - SHOW Command
-- MAGIC - DESCRIBE Command
-- MAGIC - Find the current database

-- COMMAND ----------

CREATE DATABASE demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Learning Objectives
-- MAGIC - Create mananged table using Python
-- MAGIC - Create mamaned table Using SQL
-- MAGIC - Effect of Dropping a mananed table
-- MAGIC - Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

SELECT * FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE race_resuts_sql
As
SELECT * FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM race_resuts_sql;

-- COMMAND ----------

DESC EXTENDED race_resuts_sql;

-- COMMAND ----------

DROP TABLE demo.race_resuts_sql;

-- COMMAND ----------

SHOW TABLEs IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Learning Objectives
-- MAGIC
-- MAGIC - Create External Table using Python
-- MAGIC - Create External Table using SQL
-- MAGIC - Effect of Dropping a External table
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED race_results_ext_py;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.race_results_ext_sql
(
race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP

)
USING parquet
LOCATION "/mnt/formula1dl/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;


-- COMMAND ----------

SELECT count(*) FROM demo.race_results_ext_sql;

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql;