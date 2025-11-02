# Databricks notebook source
# MAGIC %md
# MAGIC #####Databricks notebook source
# MAGIC #####Ingest lap_times folder

# COMMAND ----------

# MAGIC %md 
# MAGIC #create schema for the laptime folder's file using pyspark datafram API

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType , DoubleType

# COMMAND ----------

laptime_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", StringType(),  False)
])

# COMMAND ----------

lap_times_df = spark.read\
    .schema(laptime_schema)\
    .csv("dbfs:/mnt/formula1acstorage/raw/lap_times")
display(lap_times_df)    

# COMMAND ----------

lap_times_with_ingestion_date_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC # withColumnRenamed use for datafram

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

laptime_df = laptime_df.withColumnRenamed('raceId', "race_id")\
    .withColumnRenamed('driverId', "driver_id")\
    .withColumn("ingestion_date", current_timestamp())\
    .withColumn("data_source", lit(v_data_source))     

# COMMAND ----------

# MAGIC %md 
# MAGIC # Write the data using pyspark datafram 

# COMMAND ----------

#laptime_df.write.mode("overwrite").format('parquet').saveAsTable("f1_processed.lap_times")

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')


# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.lap_times