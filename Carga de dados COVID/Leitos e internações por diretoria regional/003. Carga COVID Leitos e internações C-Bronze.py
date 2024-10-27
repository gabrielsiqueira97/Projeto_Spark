# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

spark = SparkSession.builder \
    .appName("Carga de leitos e internações camada bronze") \
    .config("spark.sql.shuffle.partitions","2") \
    .config("spark.sql.files.maxPartitionBytes","128") \
    .config("spark.sql.parquet.compression.codec","snappy") \
    .config("spark.sql.adaptive.enable","true") \
    .getOrCreate()


# Define um número fixo de partições para shuffle, melhorando o paralelismo                 
# Define o tamanho máximo de partições para evitar muitos arquivos pequenos        
# Usa o codec Snappy para compressão rápida, otimizando tempo de leitura e escrita    
# Habilita otimizações adaptativas, ajustando o número de partições dinamicamente com base no tamanho dos dados

# Definir caminhos de armazenamento no Data Lake
lz_path_in = "/LakeHouse/landingzone/covid/leitos_internacoes/processar"
lz_path_out = "/LakeHouse/landingzone/covid/leitos_internacoes/processado"
bronze_path = "/LakeHouse/bronze/covid/leitos_internacoes"

# COMMAND ----------

schema_lz_leitos_internacoes = StructType([
    StructField("data_hora", StringType(), True), # Data no formato YYYY-MM-DD
    StructField("nome_drs", StringType(), True), # região SP DRS ou região da Grande São Paulo
    StructField("pacientes_uti_mm7d", StringType(), True),
    StructField("total_covid_uti_mm7d", StringType(), True), # Média móvel para 7 dias do Total de Leitos de UTI Destinados para COVID-19 no dia
    StructField("ocupacao_leitos", StringType(), True),
    StructField("pop", StringType(), True), # populacao_regiao_Sp População da DRS ou região da Grande São Paulo (Fonte: SEADE)
    StructField("leitos_pc", StringType(), True), # leitos_uti_por_100k_habitantes Leitos Covid-19 UTI por 100 mil habitantes (total_covid_uti_mm7d / pop)
    StructField("internacoes_7d", StringType(), True), # Número de novas internações (UTI e Enfermaria) de pacientes confirmados ou com suspeita de COVID-19 nos últimos 7 dias
    StructField("internacoes_7d_l", StringType(), True), # Número de novas internações (UTI e Enfermaria) de pacientes confirmados ou com suspeita de COVID-19 nos 7 dias anteriores
    StructField("internacoes_7v7", StringType(), True), # Variação no número de novas internações ((internacoes_7d - internacoes_7d_l) / internacoes_7d_l)
    StructField("pacientes_uti_ultimo_dia", StringType(), True),
    StructField("total_covid_uti_ultimo_dia", StringType(), True),
    StructField("ocupacao_leitos_ultimo_dia", StringType(), True),
    StructField("internacoes_ultimo_dia", StringType(), True),
    StructField("pacientes_enf_mm7d", StringType(), True),
    StructField("total_covid_enf_mm7d", StringType(), True),
    StructField("pacientes_enf_ultimo_dia", StringType(), True),
    StructField("total_covid_enf_ultimo_dia", StringType(), True)
])
#media_movel_leitos_uti_covid_7d Média móvel para 7 dias do Total de Leitos de UTI Destinados para COVID-19 no dia

df_leitos_internacoes = spark.read.option('header','true') \
    .option('delimiter',';') \
    .option('encoding','UTF-8') \
    .schema(schema_lz_leitos_internacoes) \
    .csv(lz_path_in) \
    .withColumn("nm_arquivo",regexp_extract(input_file_name(), "([^/]+)$", 0)) \
    .withColumn("dt_carga",current_date())
distinct_file_names = df_leitos_internacoes.select('nm_arquivo').distinct()

display(df_leitos_internacoes)

# COMMAND ----------

df_leitos_internacoes.write.mode('overwrite').partitionBy('nome_drs').parquet(bronze_path)

# COMMAND ----------

distinct_filenames.unpersist()
if distinct_filenames.select("nm_arquivo").distinct().count() > 0:
    filenames = distinct_filenames.select("nm_arquivo").distinct().collect()

    for row in filenames:
        src_path = row.nm_arquivo
        dbutils.fs.mv(lz_path_in + "/" + src_path, lz_path_out)
