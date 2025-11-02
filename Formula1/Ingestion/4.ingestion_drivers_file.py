# Databricks notebook source
# MAGIC %md 
# MAGIC # step 1:- Read the json file using the spark datafram reader API

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType; from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

name_schema =  StructType([
    StructField("forename", StringType(), False),
    StructField("surname",StringType(),True)
])

# COMMAND ----------

driver_schema = StructType([
    StructField("code", StringType(),True),
     StructField("driverRef", StringType(),True), 
     StructField("driverId", StringType(),True),
     StructField("name",name_schema ,True), 
    
      StructField("nationality", StringType(),True), 
       StructField("number", IntegerType(),True), 
        StructField("url", StringType(),True)

    
])

# COMMAND ----------

drivers_df =spark.read.schema(driver_schema)\
    .json(f"{raw_folder_path}/{v_file_date}/drivers.json")
display(drivers_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC step 2 : - **#Rename the json file using the spark datafram reader API**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit

# COMMAND ----------

driver_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
                                    .withColumnRenamed("driverRef", "driver_ref")\
                                    .withColumn("name",concat(col("name.forename"), lit(" "), col("name.surname")))\
                                    .withColumn("ingestion_date", current_timestamp())\
                                     .withColumn("data_source", lit(v_data_source))   
                                            
display(driver_with_columns_df)
                                        


# COMMAND ----------

# MAGIC %md
# MAGIC ##Drop the Unwanted column 

# COMMAND ----------

final_drivers_df = driver_with_columns_df.drop("url")

# COMMAND ----------

# MAGIC %md 
# MAGIC #Write the data on parquet file using Datafram API

# COMMAND ----------

drivers_df= final_drivers_df.write.mode("overwrite").format('delta').saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")