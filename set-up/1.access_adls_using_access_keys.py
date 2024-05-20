# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake Using Access Keys
# MAGIC - 1.Set the spark config fs.azure.account.key
# MAGIC - 2.List files from demo container
# MAGIC - 3.Read data from circuits.csv file

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope= 'formula1-scope', key= 'formula1dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.myformula1dl92.dfs.core.windows.net",
    formula1dl_account_key  # We can Get Access Key from Storage AC/ Access Key there we have two key...
    
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@myformula1dl92.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@myformula1dl92.dfs.core.windows.net/circuits.csv"))