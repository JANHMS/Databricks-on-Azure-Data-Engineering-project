# Databricks notebook source
# MAGIC %md ##Pyspark with Databricks FileStore
# MAGIC This notebook reads in files from the Databricks Data FileStore and does some simple Pyspark 

# COMMAND ----------

# DBTITLE 1,DataFrame
# 1 We can either create df with pandas manually and read it in with spark.
#df=spark.createDataFrame(df)

# 2.Directly read in with schema defined. 
from datetime import date, datetime

df = spark.createDataFrame([
    (1,2.,'string1', date(2000,1,1), datetime(2000,1,1,12,0)),
    (1,3.,'string2', date(2012,1,1), datetime(2000,1,1,11,0)),
], schema='a long, b double, c string, d date, e timestamp')
df

# COMMAND ----------

# DBTITLE 1,Create DataFrame with read
# Drop the file into Databricks FileStore
df = spark.read.csv('/FileStore/tables/Log.csv')

# COMMAND ----------

display(df)

# COMMAND ----------

df1 = spark.read.format('csv').option('header','true').load('/FileStore/tables/Log.csv')

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %md Now we are are having a proper table

# COMMAND ----------

df1 = spark.read.format('csv').option('header','true').option('inferSchema','true').load('/FileStore/tables/Log.csv')

# COMMAND ----------

# MAGIC %md We can see that now Time col is not a string, but interpretated as a timestamp 
# MAGIC ## Schema:
# MAGIC 
# MAGIC The schema refered to here are the column types. A column can be of type String, Double, Long, etc. Using inferSchema=false (default option) will give a dataframe where all columns are strings (StringType). Depending on what you want to do, strings may not work. For example, if you want to add numbers from different columns, then those columns should be of some numeric type (strings won't work).
# MAGIC 
# MAGIC By setting inferSchema=true, Spark will automatically go through the csv file and infer the schema of each column. This requires an extra pass over the file which will result in reading a file with inferSchema set to true being slower. But in return the dataframe will most likely have a correct schema given its input.

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.show(2)

# COMMAND ----------

# With select we can access just specific cols
df1.select('Level','Operationname').show(5)

# COMMAND ----------

# collect fetches the whole data
df1.collect()
#with take with can select the first 2 rows
df1.take(2)
#with tail we can select the last rows. 
df1.tail(2)

# COMMAND ----------

#describe df
df1.describe().show()

# COMMAND ----------

# We can also use some group by methods 
df1.goupby('Operationname').avg().show()

# COMMAND ----------

# MAGIC %md filter methods

# COMMAND ----------

df2 = df1.filter(df1.Operationname=='Delete SQL database').select('Level', 'Resourcegroup')
df2.show()

# COMMAND ----------

# DBTITLE 1,Rename Cols
df2 = df2.withColumnRenamed('Level', 'lev')
df2.show()

# COMMAND ----------

# DBTITLE 1,sort
df1.sort(df1.Time.asc()).show(5)

# COMMAND ----------

# DBTITLE 1,orderBy
df1.orderBy(df1.Time.asc()).show(5)


# COMMAND ----------

# MAGIC %md ##End of file 
