# Databricks notebook source
# MAGIC %md #Connect to Azure storage account to retreive the data

# COMMAND ----------

#With secret/createScope
aldsAccountName="endtoendproject"
adlsContainerName="endtoendproject"
adlsFolderName="data"
mountPoint="/mnt/Files/pyspark/raw"
clientSecret=dbutils.secrets.get(scope="endtoendsecrets", key="clientsec")
appId=dbutils.secrets.get(scope="endtoendsecrets",key="appId")
tenant=dbutils.secrets.get(scope="endtoendsecrets",key="tenant")

endpoint="hhtps://login.microsoftonline.com/"+tenant+"/oath2/token"
source="abfss://"+adlsContainerName+"@"+aldsAccountName+".dfs.core.windows.net/"+adlsFolderName

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": appId,
       "fs.azure.account.oauth2.client.secret": clientSecret,
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+tenant+"/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"
        }


# COMMAND ----------

# DBTITLE 1,Mount the data from blob to Databricks
dbutils.fs.mount(
source = "abfss://endtoendproject@endtoendproject.dfs.core.windows.net/data",
mount_point = mountPoint,
extra_configs = configs)

# COMMAND ----------

# MAGIC %md ##Explore the data

# COMMAND ----------

dbutils.fs.ls(mountPoint)

# COMMAND ----------

# DBTITLE 1,Read the file from the mount
from pyspark.sql.functions import *
import urllib

is_first_row_header = 'true'
read_schema = 'true'
df = spark.read.format('csv').option('header', is_first_row_header).option('interferSchema',read_schema).load(mountPoint+"/movies.csv")

# COMMAND ----------

df.show(5)

# COMMAND ----------

display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

# We can do some describe, but does not make to much sense on this data
df.describe().show()

# COMMAND ----------

# DBTITLE 1,Read ratings
df_ratings = spark.read.format('csv').option('header', is_first_row_header).option('interferSchema',read_schema).load(mountPoint+"/ratings.csv")

# COMMAND ----------

df_ratings.show(5)

# COMMAND ----------

df_links = spark.read.format('csv').option('header', is_first_row_header).option('interferSchema',read_schema).load(mountPoint+"/links.csv")

# COMMAND ----------

df_links.show(5)

# COMMAND ----------

# MAGIC %md #### We find a star schema, and we can join the tables to create a table with enriched data. 

# COMMAND ----------

df_join = df.join(df_ratings, df.movieId==df_ratings.movieId, 'inner').join(df_links, df.movieId==df_links.movieId, 'inner')


# COMMAND ----------

dbutils.fs.ls(mountPoint)

# COMMAND ----------

df_join.show(5)

# COMMAND ----------

from pyspark.sql.types import DoubleType

df_join = df_join.withColumn("rating", col("rating").cast("double"))


# COMMAND ----------

df_join.printSchema()

# COMMAND ----------

display(df_join)

# COMMAND ----------

# DBTITLE 1,Rating average per movie title
df_join.groupby('title').avg('rating').withColumnRenamed('avg(rating)', 'avg_rating').orderBy(col('avg_rating').asc()).show()

# COMMAND ----------


