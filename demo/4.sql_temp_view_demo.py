# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Dataframe using SQL
# MAGIC #### Objective
# MAGIC - 1.Create temporary view on dataframes
# MAGIC - 2.Access the view from SQL cell
# MAGIC - 3.Access the view from Python Cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_result_df.createTempView("v_race_results") # this will only work in present notebook and if we detach cluster this will also not work

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from v_race_results where race_year = 2020;

# COMMAND ----------

v_race_year = 2018

# COMMAND ----------

race_result_py = spark.sql(f"SELECT * FROM v_race_results where race_year = {v_race_year}")

# COMMAND ----------

display(race_result_py)

# COMMAND ----------

race_result_df.createOrReplaceGlobalTempView("gv_global_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.gv_global_view where race_year = 2020;

# COMMAND ----------

py_global = spark.sql("SELECT * FROM global_temp.gv_global_view").show()