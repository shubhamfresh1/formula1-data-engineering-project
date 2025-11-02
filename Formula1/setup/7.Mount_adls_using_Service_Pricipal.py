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

client_id = dbutils.secrets.get(scope='formula1-scope', key="client-id-project")
tenant_id = dbutils.secrets.get(scope='formula1-scope', key="tenant-id-project")
client_secret = dbutils.secrets.get(scope='formula1-scope', key="client-secrets-project")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id ,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@f1storage.dfs.core.windows.net/",
  mount_point = "/mnt/f1storage/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/f1storage/demo"))

# COMMAND ----------

display(spark.read.csv("dbfs:/mnt/f1storage/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/f1storage/demo")