# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: left; line-height: 0">
# MAGIC   <img src="https://sqlbits.com/images/sqlbits/2023.png" alt="Databricks Learning" style="height: 150px">
# MAGIC </div>
# MAGIC 
# MAGIC ## Silver Layer
# MAGIC ### SQLBits 2023 - Lakehouse in a Day

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Notebook
# MAGIC There are several things we need to do before we can start writing our dataframe, these are:
# MAGIC  - Import any libraries we may use
# MAGIC  - Connect to the lake storage accounts
# MAGIC  - Create & Assign widgets, the parameters that our users can supply

# COMMAND ----------

from pyspark.sql.functions import *
from delta import DeltaTable

# COMMAND ----------

# For the lake, let's setup a mount point properly - it'll throw an error if the mount already exists, so let's only do it if there's no mount already
try:
  dbutils.fs.ls("/mnt/lakehouseinaday")
except:
  configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
  }

  # Optionally, you can add <directory-name> to the source URI of your mount point.
  dbutils.fs.mount(
    source = "abfss://lake@advancinglake.dfs.core.windows.net/",
    mount_point = "/mnt/lakehouseinaday",
    extra_configs = configs)

# COMMAND ----------

dbutils.widgets.text("tableName","rolls","Table Name")
dbutils.widgets.text("sourcePath","/mnt/lakehouseinaday/dungeonmaster-sqlbits/warehouse/sparkbronze.db/rolls","Source Path")
dbutils.widgets.text("targetPath","/mnt/lakehouseinaday/dungeonmaster-sqlbits/warehouse/sparksilver.db/","Target Path")
dbutils.widgets.text("databaseName","SparkSilver","Lake Database")
dbutils.widgets.text("tableName","rolls","Lake Table")

tableName = dbutils.widgets.get("tableName")
sourcePath = dbutils.widgets.get("sourcePath")
targetPath = dbutils.widgets.get("targetPath")
databaseName = dbutils.widgets.get("databaseName")
tableName = dbutils.widgets.get("tableName")

loadUser = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframe Logic
# MAGIC Now that the notebook is set up, we can create a dataframe, apply our changes and then write the data down into our next layer

# COMMAND ----------

df = (
  spark
  .read
  .format("delta")
  .load(sourcePath)
)

# COMMAND ----------

df = (
    df
    .drop("EventEnqueuedUtcTime", "EventProcessedUtcTime", "PartitionId", "LoadUser", "LoadTimestamp")
    .filter("Roll >= 1")
)

# COMMAND ----------

#Initial Load
if not DeltaTable.isDeltaTable(spark,f"{targetPath}{tableName}"): 
    spark.sql(f'DROP TABLE IF EXISTS {databaseName}.{tableName};')
    spark.sql(f'CREATE DATABASE IF NOT EXISTS {databaseName} LOCATION "{targetPath}";')
    (
      df
      .write
      .format('delta')
      .saveAsTable(f'{databaseName}.{tableName}')
    )
else:
    targetDelta = DeltaTable.forName(spark, f'{databaseName}.{tableName}')
    
    (targetDelta.alias('target')
     .merge(df.alias('source'),
            'target.RollDateTime = source.RollDateTime and target.GameID = source.GameID and target.SystemID = source.SystemID'
           )
     .whenNotMatchedInsertAll()
     .whenMatchedUpdateAll(condition='target._RowHash <> source._RowHash')
     .execute())

# COMMAND ----------

dbutils.notebook.exit(f"Silver layer for {databaseName}.{tableName} updated successfully")