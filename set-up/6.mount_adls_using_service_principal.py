# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure Data Lake Using Service Principal
# MAGIC ### Step to Follow
# MAGIC - 1.Get client_id, tenant_id and clinet_secret from key vault
# MAGIC - 2.Set spark config with App/Clinet id, Directory/ Tenant id & Secret
# MAGIC - 3.Call file system utility mount to mount the storage
# MAGIC - 4.Explore other file system utilites related to mount (list all mount, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope= 'formula1-scope', key= 'formula1-client-id')
tenant_id = dbutils.secrets.get(scope= 'formula1-scope', key= 'formula1-tenant-id')
client_secret = dbutils.secrets.get(scope= 'formula1-scope', key= 'formula-1-clinet-secret')

# COMMAND ----------

# spark.conf.set("fs.azure.account.auth.type.myformula1dl92.dfs.core.windows.net", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type.myformula1dl92.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id.myformula1dl92.dfs.core.windows.net", client_id)
# spark.conf.set("fs.azure.account.oauth2.client.secret.myformula1dl92.dfs.core.windows.net", client_secret)
# spark.conf.set("fs.azure.account.oauth2.client.endpoint.myformula1dl92.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@myformula1dl92.dfs.core.windows.net/",
  mount_point = "/mnt/myformula1dl92/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/myformula1dl92/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/myformula1dl92/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/myformula1dl92/demo')