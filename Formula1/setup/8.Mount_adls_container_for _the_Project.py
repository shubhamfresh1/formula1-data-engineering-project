# Databricks notebook source
# MAGIC %md
# MAGIC # mount the all container from azure Blob storage gen2 and make production ready
# MAGIC 1. Register AD Application / Service principal
# MAGIC 2. Generate secrets / Passwords for the Application
# MAGIC 3. Set spark config with app / Client-ID Directory / Tenet-ID,secrets
# MAGIC 4. Assign role "Blob Storage Contributor" to delta lake

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name, container_name):

    # get secrets for vault
    client_id = dbutils.secrets.get(scope='formula1-scope', key="client-id-project")
    tenant_id = dbutils.secrets.get(scope='formula1-scope', key="tenant-id-project")
    client_secret = dbutils.secrets.get(scope='formula1-scope', key="client-secrets-project")

    # spark config
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id ,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Unmount mount if it is already mounted
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
            dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    # mount the storage container
    dbutils.fs.mount(
            source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
            mount_point = f"/mnt/{storage_account_name}/{container_name}",
            extra_configs = configs)
    
    

    display(dbutils.fs.mounts())
    

# COMMAND ----------

mount_adls('formula1acstorage', 'raw')

# COMMAND ----------

mount_adls('formula1acstorage', 'presentation')

# COMMAND ----------

mount_adls('formula1acstorage', 'processed')

# COMMAND ----------

mount_adls('formula1acstorage', 'demo')

# COMMAND ----------

dbutils.fs.ls('/mnt/formula1acstorage/demo')