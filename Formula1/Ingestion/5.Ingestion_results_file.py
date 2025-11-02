# Databricks notebook source
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

from pyspark.sql.types import *

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC #Read the data using spark datafram API

# COMMAND ----------

results_df = spark.read\
    .schema(results_schema)\
        .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp ,lit

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))


# COMMAND ----------

results_with_ingestion_date_df = add_ingestion_date(results_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = results_with_ingestion_date_df.drop(col("statusId"))



# COMMAND ----------

# MAGIC %md #####De-dupe the dataframe

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write to output to processed container in parquet format

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_processed.results;

# COMMAND ----------

#spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

results_final_df = results_deduped_df .select("result_id","driver_id", "constructor_id", "number", "grid", "position", "position_text", "position_order", "points", "laps", "time", "milliseconds", "fastest_lap", "rank", "fastest_lap_time", "fastest_lap_speed", "data_source", "file_date","ingestion_date","race_id")



# COMMAND ----------

# if (spark.catalog.tableExists("f1_processed.results")):
#     results_final_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     results_df = results_final_df.write.mode("overwrite").partitionBy("race_id").format('parquet').saveAsTable("f1_processed.results")

   

# COMMAND ----------

# results_df = final_results_df.write.mode("append").format('parquet').saveAsTable("f1_processed.results")

# COMMAND ----------

#overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_final_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, count(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING count(1) > 1
# MAGIC ORDER BY race_id, driver_id DESC;