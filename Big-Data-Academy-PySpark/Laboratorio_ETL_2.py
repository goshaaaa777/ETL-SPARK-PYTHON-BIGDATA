# Databricks notebook source
from pyspark.sql.types import StructType, StructField

#Importamos los tipos de datos que usaremos
from pyspark.sql.types import StringType, IntegerType, DoubleType

#Para importarlos todos usamos la siguiente linea
from pyspark.sql.types import *

#Importamos la librería de funciones clasicas
import pyspark.sql.functions as f

# COMMAND ----------

dfPersona= spark.sql("SELECT * FROM ejercicio_isaias.PERSONA")
dfPersona.show()

# COMMAND ----------

dfEmpresa= spark.sql("SELECT * FROM ejercicio_isaias.EMPRESA")
dfEmpresa.show()

# COMMAND ----------

dfTransaccion= spark.sql("SELECT * FROM ejercicio_isaias.TRANSACCION")
dfTransaccion.show()

# COMMAND ----------

df1 = dfTransaccion.alias("T").join(
  dfPersona.alias("P"),
  f.col("T.ID_PERSONA") == f.col("P.ID")
).select(
  f.col("P.ID").alias("ID_PERSONA"),
  f.col("P.NOMBRE"),
  f.col("P.TELEFONO"),
  f.col("P.CORREO"),
  f.col("P.FECHA_INGRESO"),
  f.col("P.EDAD"),
  f.col("P.SALARIO"),
  f.col("P.ID_EMPRESA").alias("ID_EMPRESA_TRABAJO"),
  f.col("T.ID_EMPRESA").alias("ID_EMPRESA_TRANSACCION"),
  f.col("T.MONTO"),
  f.col("T.FECHA")
)
df1.show()

# COMMAND ----------

