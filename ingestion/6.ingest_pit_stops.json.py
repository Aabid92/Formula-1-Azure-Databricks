# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pil_stops  File
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1- Read the JSON file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stop_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                             StructField("driverId", IntegerType(), True),
                             StructField("stop", StringType(), True),
                             StructField("lap", IntegerType(), True),
                             StructField("time", StringType(), True),
                             StructField("duration", StringType(), True),
                             StructField("milliseconds", IntegerType(), True)

])

# COMMAND ----------

pit_stop_df = spark.read\
    .schema(pit_stop_schema)\
    .option("multiline", True)\
    .json(f"{raw_folder_path}pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename column and add column 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

pit_stop_final_df = add_ingestion_date(pit_stop_df).withColumnRenamed("raceId", "race_id")\
                               .withColumnRenamed("driverId", "driver_id")\
                               .withColumn("data_source", lit(v_data_source))
                                

# COMMAND ----------

pit_stop_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}pit_stops")

# COMMAND ----------

display(spark.read.parquet("/mnt/myformula1dl92/processed/pit_stops"))

# COMMAND ----------

dbutils.notebook.exit("Success")