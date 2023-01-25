-- Databricks notebook source
CREATE TABLE  managed_default
    (width INT, length INT, height INT)

-- COMMAND ----------

INSERT INTO managed_default
VALUES (3, 2, 1)

-- COMMAND ----------

DESCRIBE DETAIL managed_default

-- COMMAND ----------

DESCRIBE managed_default

-- COMMAND ----------

DESCRIBE EXTENDED managed_default

-- COMMAND ----------

-- MAGIC %md Stored under the default database and the type is managed

-- COMMAND ----------

-- Storing in defined LOCATION 
CREATE TABLE  external_default
    (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_default';

INSERT INTO external_default
VALUES (3, 2, 1)

-- COMMAND ----------

DESCRIBE EXTENDED external_default

-- COMMAND ----------

-- MAGIC %md we can see that it is an external table not saved under the default location.

-- COMMAND ----------

DROP TABLE managed_default

-- COMMAND ----------

DROP TABLE external_default

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_default'

-- COMMAND ----------

-- MAGIC %md even though we dropped the table the dbfs is still there since it is external. 
-- MAGIC Since it is created outside the underlying table file is not managed by HIVE

-- COMMAND ----------

CREATE SCHEMA new_default

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED new_default

-- COMMAND ----------

-- MAGIC %md .db sowcases that is is a SCHEMA and not a table. 

-- COMMAND ----------

USE new_default;

CREATE TABLE managed_new_default
  (width INT, length INT, height INT);
  

-- COMMAND ----------


CREATE TABLE external_new_default
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_new_default';
  
INSERT INTO external_new_default
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED managed_new_default


-- COMMAND ----------

DESCRIBE EXTENDED external_new_default


-- COMMAND ----------


DROP TABLE managed_new_default;
DROP TABLE external_new_default;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/managed_new_default'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_new_default'

-- COMMAND ----------

-- MAGIC %md we can see that even though the tables were both dropped the files for the external_new_default did not drop the underlying files, since it is not managed in the default HIVE metastore. 

-- COMMAND ----------


