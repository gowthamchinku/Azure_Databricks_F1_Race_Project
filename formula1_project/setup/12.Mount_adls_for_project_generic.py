# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount Azure Data Lake containers for the project
# MAGIC

# COMMAND ----------

def mount_adls(storage_name,container_name):
    # Get the secrets from the key-vault
    client_id = dbutils.secrets.get(scope='formula1-Scope', key='formula1appclientid')
    tenant_id = dbutils.secrets.get(scope='formula1-Scope', key='formula1-app-tenantid')
    client_secret = dbutils.secrets.get(scope='formula1-Scope', key='formula1-app-clientsecret')

    #Set the spark configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # here in the any function it will check for each and every element, and if any of the element condition is true than it will execute. 
    if any(mount.mountPoint == f"/mnt/{storage_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_name}/{container_name}")


    
    # mounting code
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_name}/{container_name}",
    extra_configs = configs)


    display(dbutils.fs.mounts()) 
    
    #unmount the code
  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount the raw container

# COMMAND ----------

mount_adls('formula1dlgo','raw')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount the Processed Layer.

# COMMAND ----------

mount_adls('formula1dlgo','processed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount the presentation

# COMMAND ----------

mount_adls('formula1dlgo','presentation')b

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------


