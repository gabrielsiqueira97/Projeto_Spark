# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------


spark_delta = SparkSession.builder \
    .appName("Carga delta da camada Silver capacidade testagem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------

silver_isolamento_path = "/LakeHouse/silver/covid/capacidade_testagem/parquet"
gold_isolamento_path = "/LakeHouse/gold/covid/capacidade_testagem"

# COMMAND ----------


df_silver_isolamento_processado = spark.read.format("parquet").load(silver_isolamento_path)

tb_destino = "fato_capacidade_testagem"

df_fato_capacidade_testagem = df_silver_isolamento_processado.withColumn("sk_capacidade_testagem",monotonically_increasing_id()+1 )

df_fato_capacidade_testagem.write.format("Delta").mode("overwrite").option("overwriteSchema","true").save(f"{gold_isolamento_path}/{tb_destino}")
