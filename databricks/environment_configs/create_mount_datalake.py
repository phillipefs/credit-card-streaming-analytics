# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Connecting to Azure Data Lake Storage Gen2 [ADLS2]
# MAGIC > *creating mount points using dbfs*
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Configs Mount - DBFS
storage_name      = dbutils.secrets.get(scope="kv-databricks", key="storage-name")
client_id_app     = dbutils.secrets.get(scope="kv-databricks", key="client-id-databricks")
tenent_id_app     = dbutils.secrets.get(scope="kv-databricks", key="tenent-id-databricks")
client_secret_app = dbutils.secrets.get(scope="kv-databricks", key="secret-databricks")
containers = ["landing", "layer-bronze", "layer-silver", "layer-gold"]


configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_id_app}",
          "fs.azure.account.oauth2.client.secret": f"{client_secret_app}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenent_id_app}/oauth2/token"}

# COMMAND ----------

# DBTITLE 1,Mount DBFS ~ Azure Data Lake Storage Gen2 [ADLS2] 
def mount_datalake(containers, configs):
    try:
        for container in containers:
            dbutils.fs.mount(
                source = f"abfss://{container}@{storage_name}.dfs.core.windows.net/",
                mount_point = f"/mnt/{container}",
                extra_configs = configs
            )
            print(f" Container {container} Created....")
    except ValueError as err:
        print(err)
        

mount_datalake(containers, configs)
