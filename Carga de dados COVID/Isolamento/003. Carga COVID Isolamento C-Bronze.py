# Databricks notebook source
from pyspark.sql import SparkSession   
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------



spark = SparkSession.builder \
    .appName("Carga camada Bronze") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.files.maxPartitionBytes", "128MB") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
    
# Define um número fixo de partições para shuffle, melhorando o paralelismo                 
# Define o tamanho máximo de partições para evitar muitos arquivos pequenos        
# Usa o codec Snappy para compressão rápida, otimizando tempo de leitura e escrita    
# Habilita otimizações adaptativas, ajustando o número de partições dinamicamente com base no tamanho dos dados

# Definir caminhos de armazenamento no Data Lake
lz_path_in = "/LakeHouse/landingzone/covid/isolamento/processar"
lz_path_out = "/LakeHouse/landingzone/covid/isolamento/processado"
bronze_path = "/LakeHouse/bronze/covid/isolamento"


# COMMAND ----------


#Definir o esquema dos dados brutos
schema_lz_indice_isolamento = StructType([
    StructField("ds_municipio", StringType(), True),
    StructField("cd_municipio_ibge", IntegerType(), True),
    StructField("populacao_estimada_2020", IntegerType(), True),
    StructField("ds_uf", StringType(), True),
    StructField("data", StringType(), True),
    StructField("media_indice_isolamento", StringType(), True)
])

df_indice_isolamento = spark.read.option("header","true") \
    .option("delimiter", ";") \
    .option("encoding", "UTF-8") \
    .schema(schema_lz_indice_isolamento).csv(lz_path_in) \
    .withColumn("nm_arquivo",regexp_extract(input_file_name(), "([^/]+)$", 0)) \
    .withColumn("dt_carga",current_date())

distinct_filenames = df_indice_isolamento.select("nm_arquivo").distinct()

display(df_indice_isolamento.limit(10))

# COMMAND ----------

# Escrever a tabela no formato Parquet, particionando por Municipio
df_indice_isolamento.write.mode("overwrite").partitionBy("ds_municipio").parquet(bronze_path)

# COMMAND ----------

#distinct_filenames.unpersist()
#if distinct_filenames.select("nm_arquivo").distinct().count() > 0:
#    filenames = distinct_filenames.select("nm_arquivo").distinct().collect()

#    for row in filenames:
#        src_path = row.nm_arquivo
#        dbutils.fs.mv(lz_path_in + "/" + src_path, lz_path_out)
