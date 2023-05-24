# Databricks notebook source
dfData = spark.read.format("csv").option("header","true").option("delimiter","|").load("dbfs:/FileStore/persona.data")

# COMMAND ----------

dfData.show()

# COMMAND ----------

dfData.printSchema()

# COMMAND ----------

dfJson= spark.read.format("json").option("multiline","true").load("dbfs:/FileStore/persona.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE GOSHA.PERSONS(
# MAGIC      ID STRING,
# MAGIC      NOMBRE STRING,
# MAGIC      EDAD INT,
# MAGIC      SALARIO DOUBLE
# MAGIC  )
# MAGIC  ROW FORMAT DELIMITED
# MAGIC  FIELDS TERMINATED BY '|'
# MAGIC  LINES TERMINATED BY '\n'
# MAGIC  STORED AS TEXTFILE
# MAGIC  LOCATION '/databases/GOSHA/PERSONS'
# MAGIC   TBLPROPERTIES(
# MAGIC       'skip.header.line.count'='1',
# MAGIC       'store.charset'='ISO-8859-1', 
# MAGIC       'retrieve.charset'='ISO-8859-1'
# MAGIC   );
# MAGIC   
# MAGIC   -- Insertamos algunos datos de ejemplo
# MAGIC   INSERT INTO GOSHA.PERSONS VALUES
# MAGIC   ('1', 'Carl', 32, 20095),
# MAGIC   ('2', 'Priscilla', 34, 9298),
# MAGIC   ('3', 'Jocelyn', 27, 10853);

# COMMAND ----------

dfHive = spark.sql("SELECT * FROM GOSHA.PERSONS")

# COMMAND ----------

dfHive.show()

# COMMAND ----------



# COMMAND ----------

