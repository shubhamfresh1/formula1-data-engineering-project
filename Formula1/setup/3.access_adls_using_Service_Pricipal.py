# Databricks notebook source
# MAGIC %md
# MAGIC # Access using delta lake using SAS token
# MAGIC 1. Register AD Application / Service principal
# MAGIC 2. Generate secrets / Passwords for the Application
# MAGIC 3. Set spark config with app / Client-ID Directory / Tenet-ID,secrets
# MAGIC 4. Assign role "Blob Storage Contributor" to delta lake

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------



# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope', key="client-id-project")
tenant_id = dbutils.secrets.get(scope='formula1-scope', key="tenant-id-project")
client_secret = dbutils.secrets.get(scope='formula1-scope', key="client-secrets-project")

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.formula1acstorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1acstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1acstorage.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1acstorage.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1acstorage.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id }/oauth2/token")

