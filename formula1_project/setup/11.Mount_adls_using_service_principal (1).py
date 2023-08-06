# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount Azure Data Lake using Service Principal
# MAGIC #### Steps to achieve it
# MAGIC
# MAGIC 1. get the client_id, tenant_id and client_Secret from key_vault
# MAGIC 2. Set spark config with App/Client id, Directory/ Tenant Id and Secret
# MAGIC 3. Call file system utility and mount the storage
# MAGIC 4. Explore other file system utilities related to mount(list all mounts, unmount)

# COMMAND ----------

#dbutils.secrets.help()
dbutils.secrets.list(scope='formula1-Scope')

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-Scope', key='formula1appclientid')
tenant_id = dbutils.secrets.get(scope='formula1-Scope', key='formula1-app-tenantid')
client_secret = dbutils.secrets.get(scope='formula1-Scope', key='formula1-app-clientsecret')


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dlgo.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlgo/demo",
  extra_configs = configs)

  # here we can use the mount_point rather than using the abfss protocol.

# COMMAND ----------

        
#dbutils.fs.ls("abfss://demo@formula1dlgo.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dlgo/demo") )

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dlgo/demo/circuits.csv", header= True))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dlgo/demo")

# COMMAND ----------


