# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv File

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the spark Dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("year", IntegerType(), True),
                                     StructField("round",IntegerType(), True),
                                     StructField("circuitId", IntegerType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", DateType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("url", StringType(), True)

                                     

])

# COMMAND ----------

races_df = spark.read\
.option("header", True)\
.schema(races_schema)\
.csv(f'{raw_folder_path}races.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2- Add Ingestion date and race_timestap to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

race_with_timestamp_df = add_ingestion_date(races_df)\
                                 .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))\
                                 .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Select only Require Columns & Rename the Column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df = race_with_timestamp_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"), col("name"), col("ingestion_date"), col("race_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write Data TO Data Lake as Parquet

# COMMAND ----------

races_selected_df.write.mode("overwrite").parquet(f"{processed_folder_path}races")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Partition

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy('race_year').parquet(f"{processed_folder_path}races")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}races"))

# COMMAND ----------

dbutils.notebook.exit("Success")