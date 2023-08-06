# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake using Service Principal
# MAGIC #### Steps to achieve it
# MAGIC
# MAGIC 1. Register Azure AD application/Service Principal
# MAGIC 2. Generate a Secret/Password for the Application
# MAGIC 3. Set the spark config  for the databricks to access storage layer with Tenant ID and Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the data lake

# COMMAND ----------

"""client_id = "af8bcf70-32ce-42b3-8462-7f6dfedc3766"
tenant_id = "5808bddc-28d9-425a-871b-311bb94ca036"
client_secret = "Br68Q~AD8mJwnjDxPDde8KZLqAa6d-MupdW-Ra.i"
"""
# Here we are actually storing all these values in key-vault and access them using databricks secret utility.

client_id=dbutils.secrets.get(scope= "formula1-Scope", key="formula1appclientid")
tenant_id=dbutils.secrets.get(scope= "formula1-Scope", key="formula1-app-tenantid")
client_secret=dbutils.secrets.get(scope= "formula1-Scope", key="formula1-app-clientsecret")


# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlgo.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlgo.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlgo.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlgo.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlgo.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlgo.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlgo.dfs.core.windows.net") )

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlgo.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

 
