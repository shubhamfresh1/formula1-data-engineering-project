# Databricks notebook source
# MAGIC %md
# MAGIC # Access using delta lake using SAS token
# MAGIC 1. set spark config fs.azure.account.sas
# MAGIC 2. List  files from demo container
# MAGIC 3. Read data from curcuits.csv file

# COMMAND ----------

adls_sas_token = dbutils.secrets.get(scope='formula1-scope', key='sas-token-practice')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1acstorage.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1acstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1acstorage.dfs.core.windows.net",adls_sas_token)

# COMMAND ----------



# COMMAND ----------

