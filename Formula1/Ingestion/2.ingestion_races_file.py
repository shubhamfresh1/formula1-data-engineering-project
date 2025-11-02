# Databricks notebook source
# MAGIC %md
# MAGIC #Databricks notebook source
# MAGIC
# MAGIC ###Ingest races.csv file
# MAGIC

# COMMAND ----------

# MAGIC %run /Workspace/Formula1/includes/common_functions

# COMMAND ----------

# MAGIC %run /Workspace/Formula1/includes/configuration

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------


dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1 - Read the CSV file using the spark dataframe reader API
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

races_schema = StructType(
    [
        StructField("raceid", IntegerType(),False),
        StructField("year",IntegerType(),True),
        StructField("round",IntegerType(),True),
        StructField("circuitid",IntegerType(),True),
        StructField("name",StringType(),True),
        StructField("date",StringType(),True),
        StructField("time",StringType(),True),
        StructField("url",StringType(),True)
    ]
)

# COMMAND ----------

races_df = spark.read\
    .option("header",True)\
    .schema(races_schema)\
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import  * 

# COMMAND ----------

races_with_timestamp = races_df.withColumn("Ingestion_date", current_timestamp())\
    .withColumn(
        "race_timestamp",
        to_timestamp(
            concat(col("date"), lit(" "), col("time")),
            "yyyy-MM-dd HH:mm:ss"
        ) )\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))   

# COMMAND ----------

display(races_with_timestamp)


# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 3 - Select only the columns required & rename as required
# MAGIC

# COMMAND ----------

races_selected_df = races_with_timestamp.select(col("raceid").alias("race_id"),
                                                col("year").alias("race_year"),
                                                col("round"),
                                                col("circuitid").alias("circuit_id"),
                                                col("name"),
                                                col("Ingestion_date"),
                                                col('race_timestamp'))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write the output to processed container in parquet format
# MAGIC

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("Success")