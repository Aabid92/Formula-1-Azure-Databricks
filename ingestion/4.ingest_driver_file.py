# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest drives.json file 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)

])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                StructField("driverRef", StringType(), True),
                                StructField("number", IntegerType(), True),
                                StructField("code", StringType(), True),
                                StructField("name", name_schema),
                                StructField("dob", DateType(), True),
                                StructField("nationality", StringType(), True),
                                StructField("url", StringType(), True)
                            
                                        

])

# COMMAND ----------

drivers_df = spark.read\
    .schema(drivers_schema)\
    .json(f"{raw_folder_path}drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename column and add new columns
# MAGIC - 1. driverid rename to driver_id
# MAGIC - 2. driverRef rename to driver_ref
# MAGIC - 3. ingestion date add
# MAGIC - 4. name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

driver_with_column_df = add_ingestion_date(drivers_df).withColumnRenamed("driverId", "driver_id")\
                                  .withColumnRenamed("driverRef", "driver_ref")\
                                  .withColumn("name", concat(col("name.forename"), lit(' '), col("name.surname")))\
                                  .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drop the unwanted column
# MAGIC - 1. name.forename
# MAGIC - 2. name.surname
# MAGIC - 3. url

# COMMAND ----------

driver_final_df = driver_with_column_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4- Write file to processd in parquet format

# COMMAND ----------

driver_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}drivers")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}circuits"))

# COMMAND ----------

dbutils.notebook.exit("Success")