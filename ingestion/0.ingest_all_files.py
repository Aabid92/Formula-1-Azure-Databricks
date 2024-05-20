# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run("1.ingest_circuit_file", 0, {"p_data_source": "Ergast API"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_races_file", 0, {"p_data_source": "Ergast API"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.Ingest_constructor_file", 0, {"p_data_source": "Ergast API"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_driver_file", 0, {"p_data_source": "Ergast API"})
v_result

# COMMAND ----------

dbutils.notebook.run("5.ingest_result_file", 0, {"p_data_source": "Ergast API"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pit_stops.json", 0, {"p_data_source": "Ergast API"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_lap_times_file", 0, {"p_data_source": "Ergast API"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_qualifying_file", 0, {"p_data_source": "Ergast API"})
v_result