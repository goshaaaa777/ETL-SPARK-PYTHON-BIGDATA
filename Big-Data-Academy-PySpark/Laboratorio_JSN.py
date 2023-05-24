# Databricks notebook source
import pyspark.sql.functions as f

#Librería para crear funciones personalizadas
from pyspark.sql.functions import udf, struct

#Importamos los tipos de datos
from pyspark.sql.types import *

# COMMAND ----------

def cancularSalarioAnual(salarioMensual):
    salarioAnual = salarioMensual * 12
    return salarioAnual

# COMMAND ----------

udfCancularSalarioAnual = udf(
    cancularSalarioAnual,
    DoubleType()
    )

# COMMAND ----------

dfPersona = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
    StructType(
        [
            StructField("ID", StringType(), True),
            StructField("NOMBRE", StringType(), True),
            StructField("TELEFONO", StringType(), True),
            StructField("CORREO", StringType(), True),
            StructField("FECHA_INGRESO", StringType(), True),
            StructField("EDAD", IntegerType(), True),
            StructField("SALARIO", DoubleType(), True),
            StructField("ID_EMPRESA", StringType(), True)
        ]
    )
).load("dbfs:///FileStore/persona.data")

# COMMAND ----------

df2 = dfPersona.select(
    dfPersona["NOMBRE"].alias("NOMBRE"),
    dfPersona["SALARIO"].alias("SALARIO_MENSUAL"),
    udfCancularSalarioAnual(dfPersona["SALARIO"]).alias("SALARIO_ANUAL")
).show()

# COMMAND ----------

df3 = dfPersona.withColumn("SALARIO_ANUAL",udfCancularSalarioAnual(dfPersona["SALARIO"]))
df3.show()

# COMMAND ----------



# COMMAND ----------

