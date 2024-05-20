# Databricks notebook source
# MAGIC %md
# MAGIC ### Read all data as required

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")\
    .withColumnRenamed("number", "driver_number")\
    .withColumnRenamed("name", "driver_name")\
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructor_df = spark.read.parquet(f"{processed_folder_path}/constructors")\
    .withColumnRenamed("name", "team")

# COMMAND ----------

circuit_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
    .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")\
    .withColumnRenamed("name", "race_name")\
    .withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results")\
    .withColumnRenamed("time", "race_time")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join

# COMMAND ----------

race_circuit_df = races_df.join(circuit_df, races_df.circuit_id == circuit_df.circuit_id,"inner")\
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuit_df.circuit_location)

# COMMAND ----------

race_results_df = results_df.join(race_circuit_df, results_df.race_id == race_circuit_df.race_id)\
                             .join(drivers_df, results_df.driver_id == drivers_df.driver_id)\
                             .join(constructor_df, results_df.constructor_id == constructor_df.constructor_id) 


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position")\
    .withColumn("created_date", current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")