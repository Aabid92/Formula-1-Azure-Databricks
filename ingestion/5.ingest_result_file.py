# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Result file

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1 Read Json File

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType, FloatType

# COMMAND ----------

result_shema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                  StructField("raceId", IntegerType(), False),
                                  StructField("driverId", IntegerType(), False),
                                  StructField("constructorId", IntegerType(), False),
                                  StructField("number", IntegerType(), True),
                                  StructField("grid", IntegerType(), False),
                                  StructField("position", IntegerType(), True),
                                  StructField("positionText", StringType(), False),
                                  StructField("positionOrder", IntegerType(), True),
                                  StructField("points",FloatType() , False),
                                  StructField("laps",IntegerType() , False),
                                  StructField("time", StringType(), False),
                                  StructField("milliseconds", IntegerType(), False),
                                  StructField("fastestLap", IntegerType(), False),
                                  StructField("rank", IntegerType(), False),
                                  StructField("fastestLapTime", StringType(), False),
                                  StructField("fastestLapSpeed", FloatType(), False),
                                  StructField("statusId", StringType(), False),

])

# COMMAND ----------

result_df = spark.read\
    .schema(result_shema)\
    .json(f"{raw_folder_path}results.json")

# COMMAND ----------

display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename Column name 

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

result_rename_df = add_ingestion_date(result_df).withColumnRenamed("resultId", "result_id")\
                            .withColumnRenamed("raceId", "race_id")\
                            .withColumnRenamed("driverId", "driver_id")\
                            .withColumnRenamed("constructorId", "constructor_id")\
                            .withColumnRenamed("positionText", "position_text")\
                            .withColumnRenamed("positionOrder", "position_order")\
                            .withColumnRenamed("fastestLap", "fastest_lap")\
                            .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
                            .withColumnRenamed("fastestlapspeed", "fastest_lap_speed")\
                            .withColumn("data_source", lit(v_data_source))
                            
                               

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop unwanted column from data 

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

result_final_df = result_rename_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write file to data lake in parquet format

# COMMAND ----------

result_final_df.write.mode("overwrite").partitionBy('race_id').parquet(f"{processed_folder_path}results")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}results"))

# COMMAND ----------

dbutils.notebook.exit("Success")