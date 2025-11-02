# Databricks notebook source
# MAGIC %md 
# MAGIC #create schema for the Qualifying folder's file using pyspark datafram API

# COMMAND ----------

# MAGIC %run /Workspace/Formula1/includes/configuration

# COMMAND ----------

# MAGIC %run /Workspace/Formula1/includes/common_functions

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# MAGIC  %md ##### Step 1 - Read the JSON file using the spark dataframe reader API
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType , DoubleType

# COMMAND ----------

qualifying_schema = StructType([
    StructField("qualifyId", IntegerType(),True),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True),
  
])
   
   

# COMMAND ----------

qualifying_df = spark.read\
    .option("multiline",True)\
    .schema(qualifying_schema)\
    .json(f"{raw_folder_path}/{v_file_date}/qualifying")
display(qualifying_df)    

# COMMAND ----------

# MAGIC  %md ##### Step 2 - Rename columns and add new columns
# MAGIC  1. Rename qualifyingId, driverId, constructorId and raceId
# MAGIC  1. Add ingestion_date with current timestamp
# MAGIC

# COMMAND ----------

qualifying_with_ingestion_date_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

qualifying_df = (
    qualifying_df.withColumnRenamed('qualifyId', "qualify_id")
    .withColumnRenamed('raceId', "race_id")
    .withColumnRenamed('driverId', "driver_id")
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumn("ingestion_date", current_timestamp())\
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))        
)

# COMMAND ----------

# MAGIC  %md ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

#overwrite_partition(final_df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')


# COMMAND ----------

#qualifying_df.write.mode("overwrite").format('parquet').saveAsTable("f1_processed.qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")