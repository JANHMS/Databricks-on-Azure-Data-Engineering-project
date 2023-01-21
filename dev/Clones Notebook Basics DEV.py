# Databricks notebook source
print("Hello World")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello world FROM SQL"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hello World

# COMMAND ----------

# MAGIC %run ./Includes/Setup

# COMMAND ----------

print(name)

# COMMAND ----------

# MAGIC %fs ls 'databricks-datasets'

# COMMAND ----------

dbutils.help()

# COMMAND ----------

files = dbutils.fs.ls('databricks-datasets')
display(files)

# COMMAND ----------


