# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: left; line-height: 0">
# MAGIC   <img src="https://sqlbits.com/images/sqlbits/2023.png" alt="Databricks Learning" style="height: 150px">
# MAGIC </div>
# MAGIC 
# MAGIC ## Bronze Layer
# MAGIC ### SQLBits 2023 - Lakehouse in a Day

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Notebook
# MAGIC There are several things we need to do before we can start writing our dataframe, these are:
# MAGIC  - Import any libraries we may use
# MAGIC  - Connect to the landing and lake storage accounts
# MAGIC  - Create & Assign widgets, the parameters that our users can supply

# COMMAND ----------

#Import the python libraries we'll need in this notebook
from pyspark.sql.functions import *
from delta import DeltaTable

# COMMAND ----------

# For the lake, let's setup a mount point properly - it'll throw an error if the mount already exists, so let's only do it if there's no mount already

mountName = "/mnt/lakehouseinaday"
storageAccount = "advancinglake"
container = "lake"

try:
  dbutils.fs.ls(mountName)
except:
  configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
  }

  # Optionally, you can add <directory-name> to the source URI of your mount point.
  dbutils.fs.mount(
    source = f"abfss://{container}@{storageAccount}.dfs.core.windows.net/",
    mount_point = mountName,
    extra_configs = configs)

# COMMAND ----------

# Setup Parameters - databricks uses dbutils.widgets to create the various inputs our users can supply
dbutils.widgets.text("tableName","rolls","Table Name")
dbutils.widgets.text("landingZone","abfss://lakehouseinaday@landingspot.dfs.core.windows.net/landing/rolls/json/*/*/","Landing Location")
dbutils.widgets.text("lakePath","/mnt/lakehouseinaday/dungeonmaster-sqlbits/warehouse/sparkbronze.db/","Lake Path")
dbutils.widgets.text("fileFormat","json","Source Format")
dbutils.widgets.text("databaseName","SparkBronze","Lake Database")
dbutils.widgets.text("tableName","rolls","Lake Table")

# Assign those widgets to variables we can use in the rest of our code
tableName = dbutils.widgets.get("tableName")
fileFormat = dbutils.widgets.get("fileFormat")
landingZone = dbutils.widgets.get("landingZone")
lakePath = dbutils.widgets.get("lakePath")
databaseName = dbutils.widgets.get("databaseName")
tableName = dbutils.widgets.get("tableName")

loadUser = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframe Logic
# MAGIC Now that the notebook is set up, we can create a dataframe, apply our changes and then write the data down into our Bronze layer

# COMMAND ----------

# Create a dataframe using spark.read. We are ingesting JSON data, and we want it to work out the structure for us, so we can pass this as options
df = (
  spark
  .read
  .option("inferSchema",True)
  .option("multiLine",True)
  .format(fileFormat)
  .load(landingZone)
).selectExpr("*","_metadata as _MetaData")
# The final select piece adds a hidden system column to our dataframe, this includes the original filename each record came from!

# COMMAND ----------

# Update our dataframe to include several audit columns and a RowHash column
df = (
    df
    .withColumn('_LoadUser', lit(f'{loadUser}'))
    .withColumn('_LoadTimestamp',current_timestamp())
    .withColumn('_RowHash', 
    sha2(
        concat_ws('.',
          'Class', 
          'Dice',
          'EventEnqueuedUtcTime',
          'EventProcessedUtcTime',
          'GameID', 
          'Location',
          'Name',
          'PartitionId',
          'Race',
          'Roll',
          'SystemID'
        ),256        )
        )
)

# COMMAND ----------

# Now we want to insert the rows into our Delta table, but if the table doesn't exist, we need to create it
if not DeltaTable.isDeltaTable(spark,f"{lakePath}{tableName}"): 
    # Clean up the table & create the database - this includes a "location" so we can control where the table lives in the lake itself
    spark.sql(f'DROP TABLE IF EXISTS {databaseName}.{tableName};')
    spark.sql(f'CREATE DATABASE IF NOT EXISTS {databaseName} LOCATION "{lakePath}";')
    
    #Now write the data down as a new Delta Table - be careful with "saveAsTable" if you are not using a database with an external location specified!
    (
      df
      .write
      .format('delta')
      .saveAsTable(f'{databaseName}.{tableName}')
    )
else:
    # Create a new variable referencing the existing Delta Table
    targetDelta = DeltaTable.forName(spark, f'{databaseName}.{tableName}')
    
    # Call the Delta table MERGE function to apply the changes
    (targetDelta.alias('target')
     .merge(df.alias('source'),
            'target.RollDateTime = source.RollDateTime and target.GameID = source.GameID and target.SystemID = source.SystemID'
           )
     .whenNotMatchedInsertAll()
     .whenMatchedUpdateAll(condition='target._RowHash <> source._RowHash')
     .execute())

# COMMAND ----------

dbutils.notebook.exit(f"Bronze layer for {databaseName}.{tableName} updated successfully")