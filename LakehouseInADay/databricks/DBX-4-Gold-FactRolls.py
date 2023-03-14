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
dbutils.widgets.text("tableName","DimClass","Lake Table")

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
  df.alias("source")
  .withColumn("CharacterID",expr("(select max(CharacterID) from sparkgold.DimCharacter where source.Class = Class and source.Race = Race and source.Name = Player)"))
  .withColumn("ClassID",expr("(select max(ClassID) from sparkgold.DimClass where source.Class = Class)"))
  .withColumn("DateID",date_format(col("RollDateTime"),"yyyyMMdd").cast("int"))
  .withColumn("DiceID",expr("(select max(Dice) from sparkgold.DimDice where source.Dice = Dice)"))
  .withColumn("UserID",expr("(select max(UserID) from sparkgold.DimUsers where source.Name = UserName)"))
  .withColumn("RollTime",date_format(col("RollDateTime"),"hh:mm:ss"))
)

df = df.select("CharacterID","ClassID","DateID","DiceID","UserID","Location","RollTime","Roll")

# COMMAND ----------

if not DeltaTable.isDeltaTable(spark,f"{targetPath}{tableName.lower()}"):
  spark.sql(f'CREATE DATABASE IF NOT EXISTS {databaseName} LOCATION "{targetPath}";')
  spark.sql(f"""
    CREATE TABLE {databaseName}.{tableName} 
    (
      CharacterID bigint,
      ClassID bigint,
      DateID bigint,
      DiceID bigint,
      UserID bigint,
      Location string,
      RollTime string,
      Roll long
    )
    USING DELTA
  """)

# COMMAND ----------

targetDelta = DeltaTable.forName(spark, f'{databaseName}.{tableName}')

(targetDelta.alias('target')
 .merge(df.alias('source'),
        """target.CharacterID = source.CharacterID 
        AND target.ClassID = source.ClassID
        AND target.DateID = source.DateID
        AND target.DiceID = source.DiceID
        AND target.UserID = source.UserID
        AND target.Location = source.Location
        AND target.RollTime = source.RollTime
        """)
 .whenNotMatchedInsert(values={"CharacterID":"source.CharacterID","ClassID":"source.ClassID","DateID":"source.DateID","DiceID":"source.DiceID","UserID":"source.UserID","Location":"source.Location","RollTime":"source.RollTime","Roll":"source.Roll",})
 .execute())