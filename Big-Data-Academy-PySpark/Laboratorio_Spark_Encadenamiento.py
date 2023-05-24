# Databricks notebook source
from pyspark.sql.types import StructType, StructField

#Importamos los tipos de datos que usaremos
from pyspark.sql.types import StringType, IntegerType, DoubleType

#Para importarlos todos usamos la siguiente linea
from pyspark.sql.types import *

#Importamos la librería de funciones clasicas
import pyspark.sql.functions as f


# COMMAND ----------

dfData = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
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

dfData.show()

# COMMAND ----------

df1 = dfData.groupBy(dfData["EDAD"]).agg(
    f.count(dfData["EDAD"]).alias("CANTIDAD"),
    f.min(dfData["FECHA_INGRESO"]).alias("FECHA_CONTRATO_MAS_ANTIGUA"),
    f.sum(dfData["SALARIO"]).alias("SUMA_SALARIOS"),
    f.max(dfData["SALARIO"]).alias("SALARIO_MAYOR")
)

#Mostramos los datos
df1.show()

# COMMAND ----------

df2 = df1.filter(df1["EDAD"]>35)
df2.show()

# COMMAND ----------

df3 = df2.filter(df2["SUMA_SALARIOS"]>5000)
df3.show()

# COMMAND ----------

dfResultado = df3.filter(df3["SALARIO_MAYOR"]>1000)
dfResultado.show()

# COMMAND ----------

dfResultado = dfData.groupBy(dfData["EDAD"]).agg(
    f.count(dfData["EDAD"]).alias("CANTIDAD"),
    f.min(dfData["FECHA_INGRESO"]).alias("FECHA_CONTRATO_MAS_ANTIGUA"),
    f.sum(dfData["SALARIO"]).alias("SUMA_SALARIOS"),
    f.max(dfData["SALARIO"]).alias("SALARIO_MAYOR")
).alias("P1").\
 filter(f.col("P1.EDAD")>35).alias("P2").\
 filter(f.col("P2.SALARIO_MAYOR")>1000).alias("P3").\
 filter(f.col("P3.SALARIO_MAYOR")>1000)

dfResultado.show()
        

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

