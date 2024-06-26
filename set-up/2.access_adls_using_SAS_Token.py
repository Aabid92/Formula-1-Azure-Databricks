# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake Using SAS Token
# MAGIC - 1.Set the spark config fs.azure.account.key
# MAGIC - 2.List files from demo container
# MAGIC - 3.Read data from circuits.csv file

# COMMAND ----------

formula1_demo_sas_toeken = dbutils.secrets.get(scope= 'formula1-scope', key= 'formula1-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.myformula1dl92.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.myformula1dl92.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.myformula1dl92.dfs.core.windows.net",formula1_demo_sas_toeken)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@myformula1dl92.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@myformula1dl92.dfs.core.windows.net/circuits.csv"))