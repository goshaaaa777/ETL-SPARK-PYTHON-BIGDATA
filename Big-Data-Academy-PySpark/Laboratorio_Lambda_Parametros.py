# Databricks notebook source
import pyspark.sql.functions as f

#Librería para crear funciones personalizadas
from pyspark.sql.functions import udf, struct

#Importamos los tipos de datos
from pyspark.sql.types import *


# COMMAND ----------

def calcularNuevoSalario(salario, edad, fechaIngreso):
    resultado = 0

      if (edad > 30) & (fechaIngreso < "2008-01-01"):
            resultado = salario * 2
        else:
            resultado = salario
            
            return resultado
     

# COMMAND ----------

udfCalcularNuevoSalario = udf(
	(
		lambda parametros : calcularNuevoSalario(
			parametros[0], 
			parametros[1],
            parametros[2]
		)
	),
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
	udfcalcularNuevoSalario(
		struct(
			dfPersona["SALARIO"],
			dfPersona["EDAD"],
            dfPersona["FECHA_INGRESO"],
		)
	).alias("NUEVO_SALARIO")
)

# COMMAND ----------

df2.show()

# COMMAND ----------

