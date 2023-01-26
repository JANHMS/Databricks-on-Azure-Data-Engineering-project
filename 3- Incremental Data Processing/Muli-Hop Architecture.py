# Databricks notebook source
# MAGIC %run ./Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)


# COMMAND ----------

(spark.readStream
     .format("cloudFiles")
     .option("cloudFiles.format", "parquet")
     .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/checkpoints/orders_raw")
     .load(f"{dataset_bookstore}/orders-raw")
     .createOrReplaceTempView("orders_raw_temp")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_tmp AS (
# MAGIC   SELECT *, current_timestamp() arrival_time, input_file_name() source_file
# MAGIC   FROM orders_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_tmp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders_tmp

# COMMAND ----------

# MAGIC %md we are having 3000 entries, which we are going to write into a bronze table with spark

# COMMAND ----------

(spark.table("orders_tmp")
     .writeStream
     .format("delta")
     .option("checkPointLocation", "dbfs:/mnt/demo/checkpoints/orders_bronze")
     .outputMode("append")
     .table("order_bronze")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM order_bronze

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM order_bronze

# COMMAND ----------

# MAGIC %md the autoloader has already loaded the data into the table 

# COMMAND ----------

#static lookup table to join with our bronze table
(spark.read
    .format('json')
    .load(f"{dataset_bookstore}/customers-json")
     .createOrReplaceTempView("customers_lookup")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_lookup

# COMMAND ----------

# MAGIC %md there is the JSON opject in the proile col

# COMMAND ----------

(spark.readStream
     .table("order_bronze")
     .createOrReplaceTempView("orders_bronze_tmp")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
# MAGIC SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
# MAGIC cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, books
# MAGIC FROM orders_bronze_tmp o
# MAGIC INNER JOIN customers_lookup c
# MAGIC ON o.customer_id = c.customer_id
# MAGIC WHERE quantity > 0)

# COMMAND ----------


(spark.table("orders_enriched_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_silver")
      .outputMode("append")
      .table("orders_silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders_silver

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders_silver

# COMMAND ----------



# COMMAND ----------

# MAGIC %md We can see that we loaded new data and the streaming autoloader just "magincally" loaded the data into the silver table also making the written adjustments. 

# COMMAND ----------

(spark.readStream
    .table("orders_silver")
     .createOrReplaceTempView("orders_silver_tmp")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW daily_customer_books_tmp AS (
# MAGIC SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
# MAGIC FROM orders_silver_tmp
# MAGIC GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
# MAGIC )

# COMMAND ----------

(spark.table("daily_customer_books_tmp")
     .writeStream
     .format("delta")
     .outputMode("complete")
     .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books_tmp")
     .trigger(availableNow=True)
     .table('daily_customer_books')
)

# COMMAND ----------

# MAGIC %md we can do streaming and batch workload. We can not stream data form the gold table, since it is complete and not. astream 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM daily_customer_books

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md since it is configured as a batch job availableNow=True it does not automaticaaly update. 

# COMMAND ----------



# COMMAND ----------

#we need to rerun the batch job manually
(spark.table("daily_customer_books_tmp")
     .writeStream
     .format("delta")
     .outputMode("complete")
     .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books_tmp")
     .trigger(availableNow=True)
     .table('daily_customer_books')
)

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------


