# Databricks notebook source
# MAGIC %run /Workspace/Formula1/includes/configuration

# COMMAND ----------

race_results_df = spark.read\
.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createTempView("v_race_results")


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

p_race_year = 2019

# COMMAND ----------

race_result_2019_df = spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {p_race_year}" )

# COMMAND ----------

display(race_result_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Gobal Temp View

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.gv_race_results;

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.gv_race_results;").show()