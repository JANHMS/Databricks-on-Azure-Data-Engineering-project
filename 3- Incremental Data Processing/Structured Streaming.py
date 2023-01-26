# Databricks notebook source
# MAGIC %run ./Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders

# COMMAND ----------

spark.readStream
    .table("books")
    .createOrReplaceTempView("books_streaming_tmp_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_streaming_tmp_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS (
# MAGIC SELECT author, count(book_id) AS total_books
# MAGIC FROM books_streaming_tmp_vw
# MAGIC GROUP BY author
# MAGIC )

# COMMAND ----------

(spark.table("author_counts_tmp_vw")
     .writeStream
     .trigger(processingTime='4 seconds')
     .outputMode("complete")
     .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
     .table("author_counts")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM author_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM author_counts

# COMMAND ----------

(spark.table("author_counts_tmp_vw")
    .writeStream
    .trigger(availableNow=True)
    .outputMode("complete")
    .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
    .table("author_counts")
    .awaitTermination()
)
    

# COMMAND ----------

# MAGIC  %sql
# MAGIC SELECT * FROM author_counts

# COMMAND ----------


