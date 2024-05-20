# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap time File
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

lap_time_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                             StructField("driverId", IntegerType(), True),
                             StructField("lap", IntegerType(), True),
                             StructField("position", IntegerType(), True),
                             StructField("time", StringType(), True),
                             StructField("milliseconds", IntegerType(), True)

])

# COMMAND ----------

lab_time_df = spark.read\
    .schema(lap_time_schema)\
    .csv(f"{raw_folder_path}lap_times") # for selecting perticular file within folder we can use lap_time_file_name*.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename column and add column 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------


lap_time_final_df = add_ingestion_date(lab_time_df).withColumnRenamed("raceId", "race_id")\
                               .withColumnRenamed("driverId", "driver_id")\
                               .withColumn("data_source", lit(v_data_source))   
                                

# COMMAND ----------

lap_time_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}lap_times")

# COMMAND ----------

display(spark.read.parquet("/mnt/myformula1dl92/processed/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Success")