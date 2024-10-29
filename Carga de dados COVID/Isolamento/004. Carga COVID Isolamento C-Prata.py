# Databricks notebook source
# MAGIC %md
# MAGIC ### Camada Silver: Limpeza e Transformação
# MAGIC
# MAGIC Aplicar transformações e desnormalizar os dados na camada Silver. Use particionamento para melhorar o desempenho de leitura e escrita.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import * 

# COMMAND ----------

spark = SparkSession.builder \
    .appName("Carga isolamento camada prata") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.files.maxPartitionBytes","128MB") \
    .config("spark.sql.parquet.compression.codec","snappy") \
    .config("spark.sql.adaptive.enabled","true") \
    .getOrCreate()

# Define um número fixo de partições para shuffle, melhorando o paralelismo                 
# Define o tamanho máximo de partições para evitar muitos arquivos pequenos        
# Usa o codec Snappy para compressão rápida, otimizando tempo de leitura e escrita    
# Habilita otimizações adaptativas, ajustando o número de partições dinamicamente com base no tamanho dos dados

bronze_isolamento_path = "/LakeHouse/bronze/covid/isolamento/"
silver_isolamento_path = "/LakeHouse/silver/covid/isolamento/"


# COMMAND ----------

# MAGIC %md
# MAGIC ###Ler o dados da camada Bronze para transformação na camada Silver

# COMMAND ----------

df_bronze_isolamento = spark.read.format("parquet").load(bronze_isolamento_path)
display(df_bronze_isolamento.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Limpeza dos dados - Camada silver

# COMMAND ----------

df_isolamento_silver = df_bronze_isolamento.withColumn("dt_pesquisa",to_date(concat(regexp_replace(col("data"), ".*?\\s(\\d+/\\d+)$", "$1"), lit("/2022")), "dd/MM/yyyy")) \
        .withColumn("pct_indice_isolamento",regexp_replace(col("media_indice_isolamento"), "%", "").cast(IntegerType())) \
        .drop("nm_arquivo") \
        .drop("dt_carga") \
        .drop("Data") \
        .drop("media_indice_isolamento") \
        .withColumnRenamed("populacao_estimada_2020", "qtd_populacao_estimada")

display(df_isolamento_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gravar transformações Silver
# MAGIC
# MAGIC Particionamento por ano e mês para otimizar consultas baseadas em data, com recomendação de tamanho de arquivo em formato Parquet

# COMMAND ----------

df_isolamento_silver.withColumn("Ano",year("dt_pesquisa")) \
    .withColumn("Mes",month("dt_pesquisa")) \
    .write.option("maxRecordsPerFile",50000) \
    .partitionBy("Ano","Mes") \
    .format("parquet") \
    .mode("overwrite") \
    .save(silver_isolamento_path)

df_isolamento_silver.count()

# COMMAND ----------

#df_silver_isolamento_processado = spark.read.format("parquet").load(silver_isolamento_path)
#display(df_silver_isolamento_processado)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limpando a Memória

# COMMAND ----------

import gc
gc.collect()
