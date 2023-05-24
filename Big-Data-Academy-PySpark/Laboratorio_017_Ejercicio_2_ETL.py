# Databricks notebook source
# DBTITLE 1,1. Librerías
#Estos objetos nos ayudarán a definir la metadata
from pyspark.sql.types import StructType, StructField

#Importamos los tipos de datos que usaremos
from pyspark.sql.types import StringType, IntegerType, DoubleType

#Para importarlos todos usamos la siguiente linea
from pyspark.sql.types import *

#Importamos la librería de funciones clasicas
import pyspark.sql.functions as f

# COMMAND ----------

# DBTITLE 1,2. Lectura de datos
#Leemos el archivo indicando el esquema
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
).load("dbfs:///FileStore/DATA_PERSONA.txt")

#Mostramos los datos
dfPersona.show()

# COMMAND ----------

#Leemos el archivo indicando el esquema
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

#Leemos el archivo indicando el esquema
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

# DBTITLE 1,3. Reglas de calidad
#Reglas de calidad
dfPersonaLimpio = dfPersona.filter(
  (dfPersona["ID"].isNotNull()) &
  (dfPersona["ID_EMPRESA"].isNotNull()) &
  (dfPersona["EDAD"] > 0) &
  (dfPersona["SALARIO"] > 0)
)

#Mostramos los datos
dfPersonaLimpio.show()

# COMMAND ----------

#Reglas de calidad
dfEmpresaLimpio = dfEmpresa.filter(
  (dfEmpresa["ID"].isNotNull())
)

#Mostramos los datos
dfEmpresaLimpio.show()

# COMMAND ----------

#Reglas de calidad
dfTransaccionLimpio = dfTransaccion.filter(
  (dfTransaccion["ID_PERSONA"].isNotNull()) &
  (dfTransaccion["ID_EMPRESA"].isNotNull()) &
  (dfTransaccion["MONTO"] > 0)
)

#Mostramos los datos
dfTransaccionLimpio.show()

# COMMAND ----------

# DBTITLE 1,4. Escritura en tablas Hive
#Convertimos el dataframe en una vista temporal
dfPersonaLimpio.createOrReplaceTempView("dfPersonaLimpio")

#Lo guardamos en la tabla Hive con Spark SQL
spark.sql("""
INSERT INTO EJERCICIO_2.PERSONA
  SELECT
    *
  FROM
    dfPersonaLimpio
""")

#Verificamos
spark.sql("SELECT * FROM EJERCICIO_2.PERSONA").show()

# COMMAND ----------

#Convertimos el dataframe en una vista temporal
dfEmpresaLimpio.createOrReplaceTempView("dfEmpresaLimpio")

#Lo guardamos en la tabla Hive con Spark SQL
spark.sql("""
INSERT INTO EJERCICIO_2.EMPRESA
  SELECT
    *
  FROM
    dfEmpresaLimpio
""")

#Verificamos
spark.sql("SELECT * FROM EJERCICIO_2.EMPRESA").show()

# COMMAND ----------

#Convertimos el dataframe en una vista temporal
dfTransaccionLimpio.createOrReplaceTempView("dfTransaccionLimpio")

#Lo guardamos en la tabla Hive con Spark SQL
spark.sql("""
INSERT INTO EJERCICIO_2.TRANSACCION
  SELECT
    *
  FROM
    dfTransaccionLimpio
""")

#Verificamos
spark.sql("SELECT * FROM EJERCICIO_2.TRANSACCION").show()