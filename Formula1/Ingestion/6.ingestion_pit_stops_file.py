# Databricks notebook source
# MAGIC %md 
# MAGIC #create schema for the pitsops file using pyspark datafram API

# COMMAND ----------

# MAGIC %run /Workspace/Formula1/includes/configuration

# COMMAND ----------

# MAGIC %run /Workspace/Formula1/includes/common_functions

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType , DoubleType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")
  

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 - Rename columns and add new columns
# MAGIC #####Rename driverId and raceId
# MAGIC #####Add ingestion_date with current timestamp 
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC # withColumnRenamed use for datafram

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))     

# COMMAND ----------

# MAGIC %md 
# MAGIC # Write the data using pyspark datafram 

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# pitstop_df.write.mode("overwrite").format('parquet').saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

#overwrite_partition(final_df, 'f1_processed', 'pit_stops', 'race_id')


# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops ;