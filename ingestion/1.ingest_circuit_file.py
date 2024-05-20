# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the spark Dataframe reader

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True),

])

# COMMAND ----------

circuit_df = spark.read\
.option("header", True)\
.schema(circuits_schema)\
.csv(f'{raw_folder_path}circuits.csv')

# COMMAND ----------

type(circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Select only Require Columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuit_selected_df = circuit_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename the column name

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuit_renamed_df = circuit_selected_df.withColumnRenamed("circuitId", "circuit_id")\
                                        .withColumnRenamed("circuitRef", "circuit_ref")\
                                        .withColumnRenamed("lat", "latitude")\
                                        .withColumnRenamed("lng", "longitude")\
                                        .withColumnRenamed("alt", "altitube")\
                                        .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Add Ingestion date to the Dataframe

# COMMAND ----------

circuit_final_df = add_ingestion_date(circuit_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Write Data TO Data Lake as Parquet

# COMMAND ----------

circuit_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}circuits")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}circuits"))

# COMMAND ----------

dbutils.notebook.exit("success")