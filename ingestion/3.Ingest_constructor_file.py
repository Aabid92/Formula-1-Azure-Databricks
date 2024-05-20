# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Contructor File

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1- Read the JSON file using the spark dataframe reader 

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read\
    .schema(constructor_schema)\
    .json(f"{raw_folder_path}constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2- Drop Column from dataset 

# COMMAND ----------

constructor_drop_df = constructor_df.drop('url') # (constructor_df['url']) second method \ (col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename column and add ingesion date

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_drop_df).withColumnRenamed("constructorId", "constructor_id")\
                                          .withColumnRenamed("constructorRef", "constructor_ref")\
                                          .withColumn("data_source", lit(v_data_source))
                                              
                                

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write Output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}constructors")

# COMMAND ----------

display(spark.read.parquet("/mnt/myformula1dl92/processed/constructors"))

# COMMAND ----------

dbutils.notebook.exit("Success")