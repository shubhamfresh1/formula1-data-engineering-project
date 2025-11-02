# Databricks notebook source
spark.sql("SELECT * FROM global_temp.gv_race_results;").show()