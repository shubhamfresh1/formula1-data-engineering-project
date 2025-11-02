# Databricks notebook source
# MAGIC %md
# MAGIC # Access using delta lake using access key
# MAGIC 1. set spark config fs.azure.account.key
# MAGIC 2. List  files from demo container
# MAGIC 3. Read data from curcuits.csv file

# COMMAND ----------

formula1dl_account_key =dbutils.secrets.get(scope='formula1-scope',key= 'f1-access-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1acstorage.dfs.core.windows.net",
    formula1dl_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@storage.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1storage.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

