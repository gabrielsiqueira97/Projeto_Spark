# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

spark = SparkSession.builder \
    .appName("Carga de dados Leitos e internações camada ouro") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.files.maxPartitionBytes", "128MB") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.adaptive.anable", "true") \
    .config("spark.default.parallelism", "2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------

silver_leitos_internacoes_path = "/LakeHouse/silver/covid/leitos_internacoes"
gold_leitos_internacoes_path = "/LakeHouse/gold/covid/leitos_internacoes"

# COMMAND ----------

df_silver_leitos_internacoes = spark.read.format("parquet").load(silver_leitos_internacoes_path)
display(df_silver_leitos_internacoes.limit(10))

# COMMAND ----------

df_gold_fato_leitos_internacoes_sk = df_silver_leitos_internacoes.withColumn("sk_isolamento",monotonically_increasing_id()+1)



display(df_gold_fato_leitos_internacoes_sk.limit(10))


# COMMAND ----------

tb_destino_ft_leitos_internacoes = "fato_leitos_internacoes"

df_gold_fato_leitos_internacoes_sk.write.format("Delta") \
    .mode("overwrite") \
    .partitionBy("Ano","Mes") \
    .option("MaxRecordPerFile", 50000) \
    .option("overWriteSchema","true") \
    .save(f"{gold_leitos_internacoes_path}/{tb_destino_ft_leitos_internacoes}")

