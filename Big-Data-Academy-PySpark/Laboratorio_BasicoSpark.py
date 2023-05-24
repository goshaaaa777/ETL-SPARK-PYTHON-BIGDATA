# Databricks notebook source
from pyspark.sql.types import StructType, StructField

# COMMAND ----------

#Importamos los tipos de datos que usaremos
from pyspark.sql.types import StringType, IntegerType, DoubleType

# COMMAND ----------

#Esta librería tiene otros tipos de datos
#Para importarlos todos usamos la siguiente linea
from pyspark.sql.types import *

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

df1= dfData.select(
    dfData["ID"],
    dfData["NOMBRE"],
    dfData["EDAD"]
    )
df1.show()

# COMMAND ----------

df2=dfData.filter(dfData["EDAD"]>60)
df2.show()

# COMMAND ----------

df3 = dfData.filter(
     (dfData["EDAD"]>60) &
     (dfData["SALARIO"] >20000)

)
df3.show()

# COMMAND ----------

df4 = dfData.filter(
     (dfData["EDAD"]>60) &
     (dfData["SALARIO"] >20000)

)
df3.show()

# COMMAND ----------

df4 = dfData.filter(
     (dfData["EDAD"]>60) 
     (dfData["SALARIO"] >20000)

)
df3.show()

# COMMAND ----------

import pyspark.sql.functions as f
df5 = dfData.groupBy(dfData["EDAD"]).agg(
	f.count(dfData["EDAD"]), 
	f.min(dfData["FECHA_INGRESO"]), 
	f.sum(dfData["SALARIO"]), 
	f.max(dfData["SALARIO"])
)

#Mostramos los datos
df5.show()


# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df5 = dfData.groupBy(dfData["EDAD"]).agg(
    f.count(dfData["EDAD"]),
    f.min(dfData["FECHA_INGRESO"]),         
    f.sum(dfData["SALARIO"]) ,          
    f.max(dfData["SALARIO"]),
)
df5.show()

# COMMAND ----------

df5 = dfData.groupBy(dfData["EDAD"]).agg(
    f.count(dfData["EDAD"]).alias("CANTIDAD"),
    f.min(dfData["FECHA_INGRESO"]).alias("FECHA_CONTRADO_ANTIGUO"),         
    f.sum(dfData["SALARIO"]).alias("SALARIO_TOTAL"),          
    f.max(dfData["SALARIO"]).alias("SALARIO_MAYOR"),
)
df5.show()

# COMMAND ----------

df7 = dfData.sort(dfData["EDAD"].desc())
df7.show()

# COMMAND ----------

df8 = dfData.sort(dfData["EDAD"].asc(), dfData["SALARIO"].desc())
df8.show()

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

dfTransaccion = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
    StructType(
        [
            StructField("ID_PERSONA", StringType(), True),
            StructField("ID_EMPRESA", StringType(), True),
            StructField("MONTO", DoubleType(), True),
            StructField("FECHA", StringType(), True)
        ]
    )
).load("dbfs:///FileStore/transacciones.data")

# COMMAND ----------

dfJoin = dfTransaccion.alias("T").join(dfPersona.alias("P"),
         f.col("T.ID_PERSONA") == f.col("P.ID")
).select(
   "T.ID_PERSONA",
   "P.NOMBRE",
   "P.EDAD",
    f.col("P.ID_EMPRESA").alias("ID_EMPRESA_PERSONA_TRABAJA"),
    "P.SALARIO",
    "T.MONTO",
    "T.FECHA",
    f.col("T.ID_EMPRESA").alias("ID_EMPRESA_TRANSACCION")
)
dfJoin.show()

# COMMAND ----------



# COMMAND ----------

