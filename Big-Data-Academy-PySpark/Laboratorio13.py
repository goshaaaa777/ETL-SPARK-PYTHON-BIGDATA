# Databricks notebook source
from pyspark.sql.types  import StructType, StructField

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DoubleType

# COMMAND ----------

dfData = spark.read.format("csv").option("header",True).option("delimiter", "|").schema(
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
).load("/FileStore/persona.data")

# COMMAND ----------

df1 = dfData.select(
      dfData["ID"],
      dfData["NOMBRE"],
      dfData["EDAD"]
).show()

# COMMAND ----------

 df2 = dfData.filter(dfData["EDAD"] > 60)
 df2.show()

# COMMAND ----------

df3 = dfData.filter(
    (dfData["EDAD"] > 60 )&
    (dfData["SALARIO"] > 20000)
)
df3.show()

# COMMAND ----------

df4 = dfData.filter(
    (dfData["EDAD"] > 60 ) |
    (dfData["SALARIO"] > 20000)
)
df4.show()

# COMMAND ----------

import pyspark.sql.functions as f

df6 = dfData.groupBy(dfData["EDAD"]).agg(
	f.count(dfData["EDAD"]).alias("CANTIDAD"),
	f.min(dfData["FECHA_INGRESO"]).alias("FECHA_CONTRATO_MAS_ANTIGUA"),
	f.sum(dfData["SALARIO"]).alias("SUMA_SALARIOS"),
	f.max(dfData["SALARIO"]).alias("SALARIO_MAYOR")
)

df6.show()


# COMMAND ----------

df6.printSchema()

# COMMAND ----------

df7 = dfData.sort(dfData["EDAD"].asc())
df7.show()

# COMMAND ----------

df8 = dfData.sort(dfData["EDAD"].asc(), dfData["SALARIO"].desc())
df8.show()

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
).load("/FileStore/transacciones.data")

dfTransaccion.show()

# COMMAND ----------

dfResultado = dfData.groupBy(dfData["EDAD"]).agg(
    f.count(dfData["EDAD"]).alias("CANTIDAD"),
    f.min(dfData["FECHA_INGRESO"]).alias("FECHA_CONTRATO_MAS_ANTIGUA"),
    f.sum(dfData["SALARIO"]).alias("SUMA_SALARIOS"),
    f.max(dfData["SALARIO"]).alias("SALARIO_MAYOR")
).alias("P1").\
filter(f.col("P1.EDAD") > 35).alias("P2").\
filter(f.col("P2.SUMA_SALARIOS") > 5000).alias("P3").\
filter(f.col("P3.SALARIO_MAYOR") > 1000)

dfResultado.show()

# COMMAND ----------

dfResultado.write.mode("overwrite").format("parquet").option("compression","snappy").save("/output/dfResultado")

# COMMAND ----------

# MAGIC %fs ls /output/dfResultado

# COMMAND ----------

dfResultadoRead = spark.read.format("parquet").load("/output/dfResultado/")
dfResultadoRead.show()