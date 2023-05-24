# Databricks notebook source
from pyspark.sql.types import StructType, StructField 

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DoubleType

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

dfData = spark.read.format("csv").option("header","true").option("delimiter","|").schema(
    StructType(
        [
            StructField("ID",StringType(), True),
            StructField("NOMBRE", StringType(), True),
            StructField("TELEFONO", StringType(), True),
            StructField("CORREO", StringType(), True),
            StructField("FECHA_INGRESO", StringType(), True),
            StructField("EDAD", IntegerType(), True),
            StructField("SALARIO", DoubleType(), True),
            StructField("ID_EMPRESA", StringType(), True)    
        ]
    )
).load("dbfs:/FileStore/persona.data")

# COMMAND ----------

dfData.show()

# COMMAND ----------

dfData.printSchema()