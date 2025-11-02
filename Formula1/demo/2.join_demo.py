# Databricks notebook source
# MAGIC %run /Workspace/Formula1/includes/configuration

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits')\
            .withColumnRenamed("name", "circuit_name")\
            .withColumnRenamed("ingestion_date", "ingestion_date")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races').filter("race_year = 2019")\
    .withColumnRenamed("name", "race_name")\
    .withColumnRenamed("ingestion_date", "race_timestamp")

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df.printSchema()



# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner")\
    .select(
        circuits_df.circuit_id.alias("circuit_id"),
        circuits_df.circuit_ref.alias("location"),
        circuits_df.circuit_name.alias("country"),
        races_df.race_id.alias("race_id"),
        races_df.round.alias("round"),
    

    )

# COMMAND ----------

display(race_circuits_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #semi join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")\
    .select(
        circuits_df.circuit_id.alias("circuit_id"),
        circuits_df.circuit_ref.alias("location"),
        circuits_df.circuit_name.alias("country"),
        )

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Anti

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti")\
    .select(
        circuits_df.circuit_id.alias("circuit_id"),
        circuits_df.circuit_ref.alias("location"),
        circuits_df.circuit_name.alias("country"),
        
    

    )

# COMMAND ----------

display(race_circuits_df)