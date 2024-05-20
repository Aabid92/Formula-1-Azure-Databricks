# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuit_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
        .filter("circuit_id < 70")\
        .withColumnRenamed("name", "circuit_name")



races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019").withColumnRenamed("name", "race_name")

# COMMAND ----------

display(circuit_df)
display(races_df)

# COMMAND ----------

race_circuit_df= circuit_df.join(races_df, circuit_df.circuit_id == races_df.circuit_id, "inner")\
    .select(circuit_df.circuit_name, circuit_df.location, circuit_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

race_circuit_df.select("circuit_name").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Outer Join

# COMMAND ----------

#Left Outer Join
race_circuit_df= circuit_df.join(races_df, circuit_df.circuit_id == races_df.circuit_id, "left")\
    .select(circuit_df.circuit_name, circuit_df.location, circuit_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

#right Outer Join
race_circuit_df= circuit_df.join(races_df, circuit_df.circuit_id == races_df.circuit_id, "right")\
    .select(circuit_df.circuit_name, circuit_df.location, circuit_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

#full Outer Join
race_circuit_df= circuit_df.join(races_df, circuit_df.circuit_id == races_df.circuit_id, "full")\
    .select(circuit_df.circuit_name, circuit_df.location, circuit_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuit_df)