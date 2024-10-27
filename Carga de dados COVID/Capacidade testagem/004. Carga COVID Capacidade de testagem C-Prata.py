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

bronze_capacidade_testagem_path = "/LakeHouse/bronze/covid/capacidade_testagem"
silver_capacidade_testagem_path_parquet = "/LakeHouse/silver/covid/capacidade_testagem/parquet"
silver_capacidade_testagem_path_delta = "/LakeHouse/silver/covid/capacidade_testagem/delta"


# COMMAND ----------

# MAGIC %md
# MAGIC ###Ler o dados da camada Bronze para transformação na camada Silver

# COMMAND ----------

df_bronze_capacidade_testagem = spark.read.format("parquet").load(bronze_capacidade_testagem_path)
display(df_bronze_capacidade_testagem)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Limpeza dos dados - Camada silver

# COMMAND ----------

df_silver_capacidade_testagem = df_bronze_capacidade_testagem \
        .withColumn("vl_capacidade_nominal",col("vl_capacidade_nominal").cast(IntegerType())) \
        .drop("nm_arquivo") \
        .drop("dt_carga") \

display(df_silver_capacidade_testagem)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gravar transformações Silver
# MAGIC
# MAGIC Particionamento por ano e mês para otimizar consultas baseadas em data, com recomendação de tamanho de arquivo em formato Parquet

# COMMAND ----------


#Subindo dados Parquet
df_silver_capacidade_testagem.write.option("maxRecordsPerFile",50000) \
    .format("parquet") \
    .mode("overwrite") \
    .save(silver_capacidade_testagem_path_parquet)

df_silver_capacidade_testagem.count()

# COMMAND ----------


#Subindo arquivos Delta

spark_delta = SparkSession.builder \
    .appName("Carga delta da camada Silver capacidade testagem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


# COMMAND ----------


df_silver_isolamento_processado = spark.read.format("parquet").load(silver_capacidade_testagem_path_parquet)

tb_destino = "fato_capacidade_testagem"

df_fato_capacidade_testagem = df_silver_isolamento_processado.withColumn("sk_capacidade_testagem",monotonically_increasing_id()+1 )

df_fato_capacidade_testagem.write.format("Delta").mode("overwrite").option("overwriteSchema","true").save(f"{silver_capacidade_testagem_path_delta}/{tb_destino}")

# COMMAND ----------

display(df_fato_capacidade_testagem)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limpando a Memória

# COMMAND ----------

import gc
gc.collect()
