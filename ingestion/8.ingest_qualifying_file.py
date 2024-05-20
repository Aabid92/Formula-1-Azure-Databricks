# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifiying files
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

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                             StructField("raceId", IntegerType(), True),
                             StructField("driverId", IntegerType(), True),
                             StructField("constructorId", IntegerType(), True),
                             StructField("number", IntegerType(), True),
                             StructField("position", IntegerType(), True),
                             StructField("q1", StringType(), True),
                             StructField("q2", StringType(), True),
                             StructField("q3", StringType(), True)

])

# COMMAND ----------

qualifying_df = spark.read\
    .schema(qualifying_schema)\
    .option('multiline', True)\
    .json(f"{raw_folder_path}qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename column and add column 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------


qualifying_final_df = add_ingestion_date(qualifying_df).withColumnRenamed("qualifyId", "qualify_id")\
                                .withColumnRenamed("raceId", "race_id")\
                                .withColumnRenamed("driverId", "driver_id")\
                                .withColumnRenamed("constructorId", "constructor_id")\
                                .withColumn("data_source", lit(v_data_source))
                                

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}qualifying")

# COMMAND ----------

display(spark.read.parquet("/mnt/myformula1dl92/processed/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")