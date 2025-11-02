-- Databricks notebook source
-- MAGIC %md
-- MAGIC Databricks notebook source
-- MAGIC  
-- MAGIC   1. Write data to delta lake (managed table)
-- MAGIC   2. Write data to delta lake (external table)
-- MAGIC   3. Read data from delta lake (Table)
-- MAGIC   4. Read data from delta lake (File)

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_demo
LOCATION '/mnt/formula1acstorage/demo'


-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df = spark.read \
-- MAGIC .option("inferSchema", True) \
-- MAGIC .json("/mnt/formula1acstorage/raw/2021-03-28/results.json")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

-- COMMAND ----------

SELECT * FROM f1_demo.results_managed;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.format("delta").mode("overwrite").save("/mnt/formula1acstorage/demo/results_external")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_demo.results_external
 USING DELTA
 LOCATION "/mnt/formula1acstorage/demo/results_external"


-- COMMAND ----------

SELECT * FROM f1_demo.results_external

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

-- COMMAND ----------

SHOW PARTITIONS f1_demo.results_partitioned


-- COMMAND ----------

-- MAGIC %md
-- MAGIC  1. Update Delta Table
-- MAGIC  2. Delete From Delta Table

-- COMMAND ----------

SELECT * FROM f1_demo.results_managed;


-- COMMAND ----------

UPDATE f1_demo.results_managed
SET points = 11 - position
WHERE position <= 10


-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta.tables import DeltaTable
-- MAGIC
-- MAGIC deltaTable = DeltaTable.forPath(spark, "/mnt/formula1acstorage/demo/results_external")
-- MAGIC
-- MAGIC deltaTable.update("position <= 10", { "points": "21 - position" } )

-- COMMAND ----------

 SELECT * FROM f1_demo.results_managed;

-- COMMAND ----------

DELETE FROM f1_demo.results_managed
WHERE position > 10


-- COMMAND ----------

SELECT * FROM f1_demo.results_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Upsert using **merge****

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import upper
-- MAGIC
-- MAGIC drivers_day1_df = (
-- MAGIC     spark.read
-- MAGIC     .option("inferSchema", True)
-- MAGIC     .json("/mnt/formula1acstorage/raw/2021-03-28/drivers.json")
-- MAGIC     .filter("driverId <= 10")
-- MAGIC     .select(
-- MAGIC         "driverId",
-- MAGIC         "dob",
-- MAGIC         upper("name.forename").alias("forename"),
-- MAGIC         upper("name.surname").alias("surname")
-- MAGIC     )
-- MAGIC )
-- MAGIC
-- MAGIC display(drivers_day1_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC drivers_day1_df.createOrReplaceTempView("drivers_day1")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import upper
-- MAGIC
-- MAGIC drivers_day2_df = spark.read \
-- MAGIC .option("inferSchema", True) \
-- MAGIC .json("/mnt/formula1acstorage/raw/2021-03-28/drivers.json") \
-- MAGIC .filter("driverId BETWEEN 6 AND 15") \
-- MAGIC .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC drivers_day2_df.createOrReplaceTempView("drivers_day2")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(drivers_day2_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import upper
-- MAGIC
-- MAGIC drivers_day3_df = spark.read \
-- MAGIC .option("inferSchema", True) \
-- MAGIC .json("/mnt/formula1acstorage/raw/2021-03-28/drivers.json") \
-- MAGIC .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
-- MAGIC .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(drivers_day3_df)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
 driverId INT,
 dob DATE,
 forename STRING, 
 surname STRING,
 createdDate DATE, 
 updatedDate DATE
 )
 USING DELTA

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #Day1

-- COMMAND ----------

 MERGE INTO f1_demo.drivers_merge tgt
 USING drivers_day1 upd
 ON tgt.driverId = upd.driverId
 WHEN MATCHED THEN
   UPDATE SET tgt.dob = upd.dob,
              tgt.forename = upd.forename,
              tgt.surname = upd.surname,
              tgt.updatedDate = current_timestamp
 WHEN NOT MATCHED
   THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

-- COMMAND ----------

SELECT * FROM f1_demo.drivers_merge;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ****Day** 2**

-- COMMAND ----------

MERGE INTO f1_demo.drivers_merge tgt
 USING drivers_day2 upd
 ON tgt.driverId = upd.driverId
 WHEN MATCHED THEN
   UPDATE SET tgt.dob = upd.dob,
              tgt.forename = upd.forename,
              tgt.surname = upd.surname,
              tgt.updatedDate = current_timestamp
 WHEN NOT MATCHED
   THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

-- COMMAND ----------

SELECT  * FROM f1_demo.drivers_merge

-- COMMAND ----------

-- MAGIC  %md
-- MAGIC  #Day 3
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import current_timestamp
-- MAGIC from delta.tables import DeltaTable
-- MAGIC
-- MAGIC deltaTable = DeltaTable.forPath(spark, "/mnt/formula1acstorage/demo/drivers_merge")
-- MAGIC
-- MAGIC deltaTable.alias("tgt").merge(
-- MAGIC     drivers_day3_df.alias("upd"),
-- MAGIC     "tgt.driverId = upd.driverId") \
-- MAGIC   .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
-- MAGIC   .whenNotMatchedInsert(values =
-- MAGIC     {
-- MAGIC       "driverId": "upd.driverId",
-- MAGIC       "dob": "upd.dob",
-- MAGIC       "forename" : "upd.forename", 
-- MAGIC       "surname" : "upd.surname", 
-- MAGIC       "createdDate": "current_timestamp()"
-- MAGIC     }
-- MAGIC   ) \
-- MAGIC   .execute()
-- MAGIC

-- COMMAND ----------

-- MAGIC %sql SELECT * FROM f1_demo.drivers_merge;
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # MAGIC 1. History & Versioning
-- MAGIC # MAGIC 2. Time Travel
-- MAGIC # MAGIC 3. Vaccum

-- COMMAND ----------

DESC HISTORY f1_demo.drivers_merge

-- COMMAND ----------

SELECT * FROM f1_demo.drivers_merge VERSION AS OF 8

-- COMMAND ----------

SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2025-10-10T12:30:40.000+00:00'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df= spark.read.format("delta").option("timestampAsOf","2025-10-10T12:59:27.000+00:00").load("/mnt/formula1acstorage/demo/drivers_merge")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

VACUUM f1_demo.drivers_merge

-- COMMAND ----------

SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2025-10-10T12:30:40.000+00:00'

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM f1_demo.drivers_merge RETAIN 0 HOURS


-- COMMAND ----------

SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2025-10-10T12:30:40.000+00:00'

-- COMMAND ----------

SELECT * FROM f1_demo.drivers_merge

-- COMMAND ----------

DESC HISTORY f1_demo.drivers_merge


-- COMMAND ----------

DELETE FROM f1_demo.drivers_merge WHERE driverId = 1;

-- COMMAND ----------

SELECT *FROM f1_demo.drivers_merge VERSION AS OF 1 ;

-- COMMAND ----------

MERGE INTO f1_demo.drivers_merge tgt
USING f1_demo.drivers_merge VERSION AS OF 4 src
ON tgt.driverId = src.driverId
WHEN NOT MATCHED THEN
INSERT *

-- COMMAND ----------

MERGE INTO f1_demo.drivers_merge tgt
USING f1_demo.drivers_merge VERSION AS OF 6 src
ON (tgt.driverId = src.driverId)
WHEN NOT MATCHED THEN
INSERT *

-- COMMAND ----------

SELECT * FROM f1_demo.drivers_merge

-- COMMAND ----------

SELECT * FROM f1_demo.drivers_merge VERSION AS OF 4;

-- COMMAND ----------

 CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
 driverId INT,
dob DATE,
 forename STRING, 
 surname STRING,
 createdDate DATE, 
 updatedDate DATE
 )
 USING PARQUET

-- COMMAND ----------

INSERT INTO f1_demo.drivers_convert_to_delta
 SELECT * FROM f1_demo.drivers_merge

-- COMMAND ----------

select * from f1_demo.drivers_convert_to_delta

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("f1_demo.drivers_convert_to_delta")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("parquet").save("/mnt/formula1acstorage/demo/drivers_convert_to_delta_new")
-- MAGIC

-- COMMAND ----------


CONVERT TO DELTA parquet.`/mnt/formula1acstorage/demo/drivers_convert_to_delta_new`