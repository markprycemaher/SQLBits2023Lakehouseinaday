# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: left; line-height: 0">
# MAGIC   <img src="https://sqlbits.com/images/sqlbits/2023.png" alt="Databricks Learning" style="height: 150px">
# MAGIC </div>
# MAGIC 
# MAGIC ## Gold Layer
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
dbutils.widgets.text("sourcePath","/mnt/lakehouseinaday/dungeonmaster-sqlbits/warehouse/sparksilver.db/rolls","Source Path")
dbutils.widgets.text("targetPath","/mnt/lakehouseinaday/dungeonmaster-sqlbits/warehouse/sparkgold.db/","Target Path")
dbutils.widgets.text("databaseName","SparkGold","Lake Database")
dbutils.widgets.text("tableName","DimUsers","Lake Table")

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
  .withColumn("FirstName",split(col("Name")," ")[0])
  .withColumn("LastName",split(col("Name")," ")[1])
  .select(col("Name").alias("UserName"),"FirstName","LastName")
  .distinct()
)

# COMMAND ----------

if not DeltaTable.isDeltaTable(spark,f"{targetPath}{tableName.lower()}"):
  spark.sql(f'CREATE DATABASE IF NOT EXISTS {databaseName} LOCATION "{targetPath}";')
  spark.sql(f"""
    CREATE TABLE {databaseName}.{tableName} 
    (
      UserID bigint GENERATED ALWAYS AS IDENTITY,
      UserName string,
      FirstName string,
      LastName string
    )
    USING DELTA
  """)

# COMMAND ----------

targetDelta = DeltaTable.forName(spark, f'{databaseName}.{tableName}')

(targetDelta.alias('target')
 .merge(df.alias('source'),
        'target.UserName = source.UserName')
 .whenNotMatchedInsert(values={"UserName":"source.UserName","FirstName":"source.FirstName","LastName":"source.LastName"})
 .execute())