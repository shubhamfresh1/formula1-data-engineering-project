-- Databricks notebook source
CREATE DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

SELECT current_database() ;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT current_database() ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #MANAGE TABLE
-- MAGIC

-- COMMAND ----------

-- MAGIC
-- MAGIC %run /Workspace/Formula1/includes/configuration

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW TABLES IN demo;


-- COMMAND ----------

DESCRIBE EXTENDED race_results_python;

-- COMMAND ----------

SELECT * FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #CREATE MANAGE TABLE USING SQL

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DROP TABLE race_results_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #EXTERNAL TABLE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_ext_py;


-- COMMAND ----------

CREATE TABLE race_results_ext_sql
(
  race_year	int,
 race_name	string,
 race_date	timestamp,
  circuit_location	string,
  driver_name	string,
  driver_number	int,
  driver_nationality	string,
  team	string,
  grid	int,
  fastest_lap	int,
  race_time	string,
  points	float,
  position	int,
  created_date	timestamp
  )
  USING parquet
  LOCATION "/mnt/formula1acstorage/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES IN demo;


-- COMMAND ----------

INSERT INTO race_results_ext_sql
SELECT *
FROM  race_results_ext_py  WHERE race_year = 2020;

-- COMMAND ----------

show tables in demo;


-- COMMAND ----------

drop table demo.race_results_ext_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #VIEWS ON TABLES
-- MAGIC 1.CREATE TEMP VIEW
-- MAGIC 2.CREATE GLOBAL VIEW
-- MAGIC 3.CREATE PERMANENT VIEW

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #temp view

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM v_race_results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Global View

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW gv_race_results
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2012;

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

CREATE OR REPLACE VIEW pv_race_results
AS 
SELECT *
FROM demo.race_results_python
WHERE race_year = 2000;

-- COMMAND ----------

SELECT * FROM pv_race_results;

