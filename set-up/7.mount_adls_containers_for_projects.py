# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure Data Lake container for projects

# COMMAND ----------

def mount_adls(storage_account, container_name):
    # Get the secret for Key Vault
    client_id = dbutils.secrets.get(scope= 'formula1-scope', key= 'formula1-client-id')
    tenant_id = dbutils.secrets.get(scope= 'formula1-scope', key= 'formula1-tenant-id')
    client_secret = dbutils.secrets.get(scope= 'formula1-scope', key= 'formula-1-clinet-secret')

    # Set Spark Configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    if any(mount.mountPoint == f"/mnt/{storage_account}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account}/{container_name}")
    
    # Mount the storage account containers 
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account}/{container_name}",
      extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('myformula1dl92','demo')

# COMMAND ----------

mount_adls('myformula1dl92', 'raw')

# COMMAND ----------

mount_adls('myformula1dl92', 'processed')

# COMMAND ----------

mount_adls('myformula1dl92', 'presentation')