-- Databricks notebook source
-- MAGIC %fs mkdirs /schemas

-- COMMAND ----------

CREATE TABLE PRUEBA.PERSONA_TXT(
    ID STRING,
    NOMBRE STRING,
    TELEFONO STRING,
    CORREO STRING,
    FECHA_INGRESO DATE,
    EDAD INT,
    SALARIO DOUBLE,
    ID_EMPRESA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/databases/PRUEBA/PERSONA_TXT'
TBLPROPERTIES(
    'skip.header.line.count'='1',
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1'
);

-- COMMAND ----------

-- MAGIC %fs cp /FileStore/persona_1.data /databases/PRUEBA/PERSONA_TXT

-- COMMAND ----------

select * from PRUEBA.PERSONA_TXT limit 10;

-- COMMAND ----------

-- MAGIC %fs cp /FileStore/persona_1.avsc /schemas

-- COMMAND ----------

-- MAGIC %fs mv /schemas/persona_1.avsc  /schemas/persona.avsc

-- COMMAND ----------

-- MAGIC %fs ls /schemas

-- COMMAND ----------

--tabla avro
CREATE TABLE PRUEBA.PERSONA_AVRO
STORED AS AVRO
LOCATION 'databases/PRUEBA/PERSONA_AVRO'
TBLPROPERTIES(
  'store.charset'='ISO-8859-1',
  'retrieve,charset'='ISO-8859-1',
  'avro.schema.url'='dbfs:/schemas/persona.avsc',
  'avro.output.codec'='snappy'
)

-- COMMAND ----------

SET hive.exec.compress.output=true;
SET avro.output.code=snappy;

-- COMMAND ----------

INSERT INTO TABLE PRUEBA.PERSONA_AVRO
SELECT * FROM PRUEBA.PERSONA_TXT;

-- COMMAND ----------

SELECT COUNT(*) FROM prueba.persona_avro;

-- COMMAND ----------

-- MAGIC %fs ls /databases/PRUEBA/PERSONA_AVR

-- COMMAND ----------

DROP TABLE PRUEBA.PERSONA_TXT

-- COMMAND ----------

-- MAGIC %fs rm -r /databases/PRUEBA/PERSONA_TXT

-- COMMAND ----------

CREATE TABLE PRUEBA.PERSONA_TXT(
    ID STRING,
    NOMBRE STRING,
    HIJOS INT,
    TELEFONO STRING,
    CORREO STRING,
    FECHA_INGRESO DATE,
    EDAD INT,
    SALARIO DOUBLE,
    ID_EMPRESA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/databases/PRUEBA/PERSONA_TXT'
TBLPROPERTIES(
    'skip.header.line.count'='1',
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1'
);

-- COMMAND ----------

-- MAGIC %fs cp /FileStore/persona_2.data /databases/PRUEBA/PERSONA_TXT

-- COMMAND ----------

SELECT COUNT(*) FROM PRUEBA.PERSONA_TXT LIMIT 10;

-- COMMAND ----------

-- MAGIC %fs rm /schemas/persona.avsc

-- COMMAND ----------

-- MAGIC %fs cp /FileStore/persona_2.avsc /schemas/

-- COMMAND ----------

-- MAGIC %fs mv /schemas/persona_2.avsc /schemas/persona.avsc

-- COMMAND ----------

-- MAGIC %fs ls /schemas

-- COMMAND ----------

SET hive.exec.compress.output=true;
SET avro.output.codec=snappy;

-- COMMAND ----------

INSERT INTO TABLE PRUEBA.PERSONA_AVRO
SELECT * FROM PRUEBA.PERSONA_TXT;

-- COMMAND ----------

-- MAGIC %fs ls /databases/PRUEBA/PERSONA_AVRO

-- COMMAND ----------

