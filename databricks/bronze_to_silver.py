# Databricks notebook source
configs = {
   "fs.adl.oauth2.access.token.provider.type": "CustomAccessTokenProvider",
   "fs.adl.oauth2.access.token.custom.provider": spark.conf.get("spark.databricks.passthrough.adls.tokenProviderClassName")
}

dbutils.fs.mount(
   source = "adl://bronze@adlsgen2devimal.azuredatalakestore.net/",
   mount_point = "/mnt/bronze",
   extra_configs = configs
)

# COMMAND ----------

configs = {
   "fs.adl.oauth2.access.token.provider.type": "CustomAccessTokenProvider",
   "fs.adl.oauth2.access.token.custom.provider": spark.conf.get("spark.databricks.passthrough.adls.tokenProviderClassName")
}

dbutils.fs.mount(
   source = "adl://silver@adlsgen2devimal.azuredatalakestore.net/",
   mount_point = "/mnt/silver",
   extra_configs = configs
)

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/")

# COMMAND ----------

spark.read.format("csv").load("abfss://bronze@adlsgen2devimal.dfs.core.windows.net/house_price").collect()

# COMMAND ----------



# Now try reading the file again
df = spark.read.format("csv") \
    .load("abfss://bronze@adlsgen2devimal.dfs.core.windows.net/house_price")
display(df)

# COMMAND ----------


