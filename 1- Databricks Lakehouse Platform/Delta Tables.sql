-- Databricks notebook source
CREATE TABLE employees
  (id INT, name STRING, salary DOUBLE)

-- COMMAND ----------

INSERT INTO employees
VALUES
  (1, 'Jan', 3000),
  (2, 'Jan1', 2000),
  (3, 'Jan2', 1000),
  (4, 'Jan3', 2000),
  (5, 'Jan4', 3000),
  (6, 'Jan5', 1000)

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

DESCRIBE DETAIL employees


-- COMMAND ----------

-- MAGIC %fs dbfs:/user/hive/warehouse/employees

-- COMMAND ----------

UPDATE employees
SET salary = salary + 100
WHERE name LIKE '%Jan%'

-- COMMAND ----------

SELECT * FROM employees


-- COMMAND ----------

-- MAGIC %fs dbfs:/user/hive/warehouse/employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log'

-- COMMAND ----------

-- MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000000.crc'

-- COMMAND ----------


