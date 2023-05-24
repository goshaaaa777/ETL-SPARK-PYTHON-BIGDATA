# Databricks notebook source
dfData = spark.read.format("csv").option("header", "true").option("delimiter","|").load("/FileStore/persona.data")

# COMMAND ----------

dfData.show()

# COMMAND ----------

dfData.printSchema()

# COMMAND ----------

dfJson = spark.read.format("json").option("multiLine",True).load("/FileStore/persona.json")

# COMMAND ----------

dfJson.show()

# COMMAND ----------

dfJson.printSchema()

# COMMAND ----------

