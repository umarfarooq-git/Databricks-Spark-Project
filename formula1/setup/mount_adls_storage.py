# Databricks notebook source
dbutils.secrets.list('formula1-scope')

# COMMAND ----------

# Databricks notebook source
storage_account_name = "formula1dlake3"
client_id = dbutils.secrets.get(scope="formula1-scope", key="client-id")
tenant_id = dbutils.secrets.get(scope="formula1-scope", key="tenant-id")
client_secret = dbutils.secrets.get(scope="formula1-scope", key="client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# Function to mount containers
def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

# Mount both containers via conveniant function call.
mount_adls("processed")
mount_adls("raw")

# COMMAND ----------

# Check the list of available mounts
dbutils.fs.mounts()

# COMMAND ----------

# LS the both containers just to make sure
dbutils.fs.ls("/mnt/formula1dlake3/processed")


# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dlake3/raw")