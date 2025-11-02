# Databricks notebook source
# MAGIC %run /Workspace/Formula1/includes/configuration

# COMMAND ----------

# MAGIC %md 
# MAGIC #Aggration Functions demo

# COMMAND ----------

reces_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(reces_result_df)

# COMMAND ----------

demo_df = reces_result_df.filter("race_year = 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count , countDistinct ,sum , avg, max ,min

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(count("race_name")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------



# COMMAND ----------

demo_df.select(avg("points")).show()


# COMMAND ----------

demo_df.select(max("points")).show()

# COMMAND ----------

demo_df.select(min("points")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton' ").select(sum("points")).show()

# COMMAND ----------

demo_df\
.groupBy("driver_name")\
    .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")).show()

# COMMAND ----------

# MAGIC %md 
# MAGIC #window 

# COMMAND ----------

demo_df = reces_result_df.filter("race_year in (2019,2020)")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_groped_df = demo_df\
.groupBy("race_year", "driver_name")\
    .agg(sum("points"), countDistinct("race_name")).show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc ,rank

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
