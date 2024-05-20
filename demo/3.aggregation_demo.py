# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df = race_results_df.filter("race_year = 2020")

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum, grouping,desc, rank

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

# demo_df.select(sum("points")).show()
demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points"), countDistinct("race_name"))\
    .withColumnRenamed("sum(points)", "total_points")\
    .withColumnRenamed("count(DISTINCT race_name)", "number_of_races")\
    .show()

# COMMAND ----------

demo_df\
    .groupBy("driver_name")\
    .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_race"))\
    .orderBy(desc("total_points"))\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Windows functions

# COMMAND ----------

demo_df = race_results_df.filter("race_year in (2019, 2020)")

# COMMAND ----------

demo_grp_df = demo_df\
    .groupBy("race_year","driver_name")\
    .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_race"))
    

# COMMAND ----------

display(demo_grp_df)

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

driver_rank = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_grp_df.withColumn("rank", rank().over(driver_rank)).show(20)