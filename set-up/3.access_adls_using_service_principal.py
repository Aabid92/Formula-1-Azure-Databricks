# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake Using Service Principal
# MAGIC - 1.Register Azure AD Application / Service Principal
# MAGIC - 2.Generate a secret or Password for Application
# MAGIC - 3.Set Spark Config with App/ Client Id, Directory/ tenant Id & Secret
# MAGIC - 4.Assign Role Storage Blob Data Container to Data Lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope= 'formula1-scope', key= 'formula1-client-id')
tenant_id = dbutils.secrets.get(scope= 'formula1-scope', key= 'formula1-tenant-id')
client_secret = dbutils.secrets.get(scope= 'formula1-scope', key= 'formula-1-clinet-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.myformula1dl92.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.myformula1dl92.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.myformula1dl92.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.myformula1dl92.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.myformula1dl92.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@myformula1dl92.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@myformula1dl92.dfs.core.windows.net/circuits.csv"))