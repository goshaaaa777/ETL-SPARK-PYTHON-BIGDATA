# Databricks notebook source
from pyspark.sql.types  import StructType, StructField

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DoubleType

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

dfPersona = spark.read.format("csv").option("header",True).option("delimiter", "|").schema(
StructType([
    StructField("ID",StringType(),True),
    StructField("NOMBRE",StringType(),True),
    StructField("TELEFONO",StringType(),True),
    StructField("CORREO",StringType(),True),
    StructField("FECHA_INGRESO",StringType(),True),
    StructField("EDAD",IntegerType(),True),
    StructField("SALARIO",DoubleType(),True),  
    StructField("ID_EMPRESA",StringType(),True)   
    ]
)
#Mostramos los datos

).load("/FileStore/persona.data")

# COMMAND ----------

dfData.show()

# COMMAND ----------

dfData.printSchema()

# COMMAND ----------

dfEmpresa = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
    StructType(
        [
            StructField("ID", StringType(), True),
            StructField("NOMBRE", StringType(), True)
        ]
    )
).load("dbfs:///FileStore/DATA_EMPRESA.txt")

#Mostramos los datos
dfEmpresa.show()

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
).load("dbfs:///FileStore/DATA_TRANSACCION.txt")

#Mostramos los datos
dfTransaccion.show()

# COMMAND ----------

#Reglas de Calidad para Persona
dfPersonaLimpio = dfPersona.filter(
  (dfPersona["ID"].isNotNull()) &
  (dfPersona["ID_EMPRESA"].isNotNull()) &
  (dfPersona["EDAD"] > 0) &
  (dfPersona["SALARIO"] > 0)
)
#Mostramos los datos
dfPersonaLimpio.show()

# COMMAND ----------

#Reglas de calidad para la Empresa
dfEmpresaLimpio = dfEmpresa.filter(
  (dfEmpresa["ID"].isNotNull())
)
#Mostramos los datos
dfEmpresaLimpio.show()

# COMMAND ----------

#Reglas de calidad para Transacciones
dfTransaccionLimpio = dfTransaccion.filter(
  (dfTransaccion["ID_PERSONA"].isNotNull()) &
  (dfTransaccion["ID_EMPRESA"].isNotNull()) &
  (dfTransaccion["MONTO"] > 0)
)

#Mostramos los datos
dfTransaccionLimpio.show()

# COMMAND ----------

#Convertimos el dataframe en una vista temporal
dfTransaccionLimpio.createOrReplaceTempView("dfTransaccionLimpio")

#Lo guardamos en la tabla Hive con Spark SQL
spark.sql("""
INSERT INTO PROYECTO.TRANSACCION
  SELECT
    *
  FROM
    dfTransaccionLimpio
""")

#Verificamos
spark.sql("SELECT * FROM PROYECTO.TRANSACCION").show()

# COMMAND ----------

dfEmpresaLimpio.createOrReplaceTempView("dfEmpresaLimpio")
#Lo guardamos en la tabla Hive con Spark SQL
spark.sql("""
INSERT INTO PROYECTO.EMPRESA
  SELECT
    *
  FROM
    dfEmpresaLimpio
""")

#Verificamos
spark.sql("SELECT * FROM PROYECTO.EMPRESA").show()

# COMMAND ----------

dfPersonaLimpio.createOrReplaceTempView("dfPersonaLimpio")

#Lo guardamos en la tabla Hive con Spark SQL
spark.sql("""
INSERT INTO PROYECTO.PERSONA
  SELECT
    *
  FROM
    dfPersonaLimpio
""")

#Verificamos
spark.sql("SELECT * FROM PROYECTO.PERSONA").show()