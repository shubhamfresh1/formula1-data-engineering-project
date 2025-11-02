# Databricks notebook source
# MAGIC %run /Workspace/Formula1/includes/configuration

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_filter_df = races_df.filter("race_year == 2019 and round <= 5").display()