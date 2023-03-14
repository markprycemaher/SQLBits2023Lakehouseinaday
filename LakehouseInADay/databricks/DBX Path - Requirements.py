# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: left; line-height: 0">
# MAGIC   <img src="https://sqlbits.com/images/sqlbits/2023.png" alt="Databricks Learning" style="height: 150px">
# MAGIC </div>
# MAGIC 
# MAGIC ## Lakehouse In A Day
# MAGIC ### Working with Databricks
# MAGIC Welcome to the Databricks learning path, part of SQLBits 2023's Lakehouse In A Day session
# MAGIC 
# MAGIC The solution included here has several considerations before you can begin, this notebook serves as a guide to get you started!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initial Setup
# MAGIC 
# MAGIC There are a lot of decisions to be made when setting up Databricks and it's connectivity to various storage accounts. We're going to keep things easy today by using Active Directory passthrough. Databricks are moving away from this approach with Unity Catalog being introduced, but that requires setup on your Azure Tenant, and we're skipping today for today!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cluster Requirements
# MAGIC You'll need a cluster setup to work with in Databricks. We're not dealing with huge amounts of data, so we can keep things small.
# MAGIC 
# MAGIC Navigate to the "Compute" tab and create a new cluster with the following settings:
# MAGIC  - Single Node
# MAGIC  - Access Mode: Single User Access
# MAGIC  - Runtime: 12.2
# MAGIC  - Node Type: Standard_DS3_v2
# MAGIC  - Terminate after: 30 minutes
# MAGIC  - Advanced Options > Enable credential passthrough for user-level data access ticked
# MAGIC 
# MAGIC In order to use AD Passthrough, you must by using a Premium Databricks workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storage Account Setup
# MAGIC 
# MAGIC Now that we've got a cluster that can use AD Passthrough, you'll need to make sure your AD user actually has access!
# MAGIC 
# MAGIC For both your landing storage account, and your lake storage account, ensure you have access with the following steps:
# MAGIC 
# MAGIC  - Navigate to the storage account in the Azure Portal
# MAGIC  - Under Containers, navigate to the container you want to use
# MAGIC  - Click on "Access Control (IAM)"
# MAGIC  - Add a new Role Assignment
# MAGIC  - Select the "Storage Blob Data Contributor" option
# MAGIC  - Add yourself as the relevant user
# MAGIC  - Confirm and apply the new role
# MAGIC  
# MAGIC Perform the above for both storage accounts and you should be good to go!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Testing Data Access
# MAGIC When you want to test your access, you can try a "directory listing" pyspark command
# MAGIC 
# MAGIC `dbutils.fs.ls("abfss://<container>@<storage account>.dfs.core.windows.net")`
# MAGIC 
# MAGIC This should bring back a list of files/folders in the location - if it throws an error, the user permissions are not quite right!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mounting Storage
# MAGIC This is another practice that is superseded by Unity Catalog, but a common practice is to mount a storage account. This tells Databricks to remember the connection method to a storage account, and keep a shortcut directly to it.
# MAGIC 
# MAGIC To keep things easy, you can use the following script which calls `dbutils.fs.mount()` if the storage has not been mounted, you can include this at the start of each notebook

# COMMAND ----------

mountName = "/mnt/lakehouseinaday"
storageAccount = "your storage account name"
container = "your adls container name"

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

# MAGIC %md
# MAGIC ## Pyspark Cheatsheet
# MAGIC 
# MAGIC If this is your first time writing pyspark, it can be a little intimidating! But don't worry, there are some common building blocks, and once you've learned them, you'll find it's not so strange!
# MAGIC 
# MAGIC We ran through the basics of reading files and writing files in the introduction deck, so let's look at some other common areas

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading JSON
# MAGIC The Databricks documentation on reading JSON is great, check it out here:
# MAGIC 
# MAGIC https://docs.databricks.com/external-data/json.html

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters
# MAGIC If you want to include parameters in your notebook, you can use the following commands:
# MAGIC 
# MAGIC `dbutils.widgets.text(<widget name>,<default value>,<label for users>)`
# MAGIC 
# MAGIC Running this code will create a new text box for the user to enter values into
# MAGIC 
# MAGIC `myValue = dbutils.widgets.get(<widget name>)`
# MAGIC 
# MAGIC This command returns the current value of a widget into a local variable that you can use in the rest of your code
# MAGIC 
# MAGIC Read more on widgets here:
# MAGIC https://docs.databricks.com/notebooks/widgets.html

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming Data in Pyspark
# MAGIC 
# MAGIC For most transformations, we are adding/replacing an existing column. To do this we use the withColumn function:
# MAGIC 
# MAGIC `df = df.withColumn(<column name>, <transformations>)`
# MAGIC 
# MAGIC There are LOADS of pyspark transformation commands, and the documentation is pretty good these days:
# MAGIC 
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html
# MAGIC 
# MAGIC To use these functions, you must first import the pyspark SQL functions
# MAGIC 
# MAGIC `from pyspark.sql.functions import *`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming Data with SQL
# MAGIC 
# MAGIC Struggling with the huge variety of Pyspark transformations? Don't worry - everyone does! It's good to practice with pyspark, but lots of times we just want to write SQL instead. There are a couple of tricks to using SQL to speed things up:
# MAGIC 
# MAGIC #### Selecting Data using SQL
# MAGIC If we have previously registered our data as a SQL Table, we can query it directly into a pyspark dataframe
# MAGIC 
# MAGIC `df = spark.table(<table name>)` - this brings a whole table back as a data frame
# MAGIC 
# MAGIC `df = spark.sql(<sql statement>)` - this lets us write a full SQL select statement, and creates the dataframe based on that logic
# MAGIC 
# MAGIC #### Adding/Updating Columns using SQL
# MAGIC 
# MAGIC If we can't write full SQL select statements, we can write SQL column definitions instead, this uses the expr() function, which is incredibly powerful!
# MAGIC 
# MAGIC `df = df.withColumn(<column name>,expr(<sql column definition>))`

# COMMAND ----------

# For example, if we had a dataframe with the column "rejectFlag", to be updated whenever our CustomerID is null, we could write:

from pyspark.sql.functions import *

df = df.withColumn("rejectFlag", expr("CASE WHEN CustomerID IS NULL THEN 1 ELSE 0 END"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Auditing approaches
# MAGIC When performing ETL, there are lots of common practices - tagging records with their source file, the load date, the user who executed the commands etc. The following pyspark commands will be very useful - use them within a withColumn() command for auditing!
# MAGIC 
# MAGIC  - `current_timestamp()` - returns the current timestamp
# MAGIC  - `sha2([columnlist],<size>)` - creates a hash over several columns for composite ID creation
# MAGIC  - `dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')` - returns the current user
# MAGIC 
# MAGIC Another very useful tip is to expose the hidden `_Metadata` column, you can do this using a `selectExpr()` command which accepts a list of SQL strings and turns it into a dataframe. The below command takes all existing columns ("*"), then exposes the metadata as a new column called "_FileMetadata"
# MAGIC 
# MAGIC 
# MAGIC  - `df = df.selectExpr("*","_metadata as _FileMetadata")`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merging Data using Delta
# MAGIC We are using Delta as the primary storage format for all lake data, that means we can use the merge functionality to upsert rows!
# MAGIC 
# MAGIC You can use SQL to write a merge statement exactly how you would normally in T-SQL, but we can also write very flexible dynamic merge statements using python.
# MAGIC 
# MAGIC If using python, you'll need to import the delta tables library!
# MAGIC 
# MAGIC `from delta.tables import *`
# MAGIC 
# MAGIC 
# MAGIC For updating, deleting and merging in Delta, check out the Delta documentation:
# MAGIC https://docs.delta.io/latest/delta-update.html

# COMMAND ----------

# A quick example of merging in Delta - applying updates from "dfUpdates" to our existing table "silver.existing"

# Import the delta library
from delta.tables import *

dfUpdates = spark.table("bronze.updates")

existingDelta = DeltaTable.forName(spark, 'silver.existing')

(existingDelta.alias('existing')
  # The dataframe to merge into our target, and how to evaluate matches
  .merge(dfUpdates.alias('updates'),'existing.id = updates.id')
  # Columns to update when rows match
  .whenMatchedUpdate(set = {"id": "updates.id","name": "updates.name"} )
  # Columns to insert for source rows that do not match
  .whenNotMatchedInsert(values = {"id": "updates.id","name": "updates.name"} )
  .execute()
)