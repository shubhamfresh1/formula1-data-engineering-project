# Databricks notebook source
# MAGIC %md
# MAGIC # Read the json file using the spark datafram reader

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %fs
# MAGIC ls /mnt/formula1acstorage/raw

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

constructor_schema = StructType([
    StructField("ConstructorID", IntegerType(), False),
    StructField("ConstructorRef", StringType(),True),
    StructField("name", StringType(),True),
    StructField("nationality",StringType(),True),
    StructField("url", StringType(),True)
]
)

# COMMAND ----------

constructor_df = spark.read \
.json(f'{raw_folder_path }/{v_file_date}/constructors.json')


# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))



# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('_corrupt_record'))

# COMMAND ----------

display(constructor_dropped_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())\
                                              .withColumn("data_source", lit(v_data_source))\
                                             .withColumn("file_date", lit(v_file_date))   


# COMMAND ----------

# MAGIC %md
# MAGIC # write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format('delta').saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")