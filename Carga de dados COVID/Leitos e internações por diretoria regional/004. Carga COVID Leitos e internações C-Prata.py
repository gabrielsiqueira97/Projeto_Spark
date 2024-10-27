# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------



spark = SparkSession.builder \
    .appName("Carga de dados Leitos e internações camada Prata") \
    .config("spark.sql.shuffle.partitions", "3")  \
    .config("spark.sql.files.maxPartitionBytes", "128MB") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define um número fixo de partições para shuffle, melhorando o paralelismo                 
# Define o tamanho máximo de partições para evitar muitos arquivos pequenos        
# Usa o codec Snappy para compressão rápida, otimizando tempo de leitura e escrita    
# Habilita otimizações adaptativas, ajustando o número de partições dinamicamente com base no tamanho dos dados

bronze_leitos_internacoes_path = "/LakeHouse/bronze/covid/leitos_internacoes"
silver_leitos_internacoes_path = "/LakeHouse/silver/covid/leitos_internacoes"


# COMMAND ----------

df_bronze_leitos_internacoes = spark.read.format("parquet").load(bronze_leitos_internacoes_path)


# COMMAND ----------

display(df_bronze_leitos_internacoes.limit(10))

# COMMAND ----------

 
df_prata_leitos_internacoes = df_bronze_leitos_internacoes \
    .withColumnRenamed("nome_drs", "nm_regiao") \
    .withColumn("dt_internacao", to_date(col("data_hora"), "yyyy-MM-dd")) \
    .withColumn("mm7d_pacientes_uti", round(regexp_replace(col("pacientes_uti_mm7d"), ",", ".").cast("double"), 2)) \
    .withColumn("mm7d_total_covid_uti", round(regexp_replace(col("total_covid_uti_mm7d"), ",", ".").cast("double"), 2)) \
    .withColumn("ptc_ocupacao_leitos", round(regexp_replace(col("ocupacao_leitos"), ",", ".").cast("double"), 2)) \
    .withColumn("nr_leitos_100k_habitantes", round(regexp_replace(col("leitos_pc"), ",", ".").cast("double"), 2)) \
    .withColumn("nr_internacoes_7d", col("internacoes_7d").cast("int")) \
    .withColumn("nr_internacoes_7d_l", col("internacoes_7d_l").cast("int")) \
    .withColumn("nr_internacoes_7v7", regexp_replace(col("internacoes_7v7"), ",", ".").cast("double")) \
    .withColumn("nr_pacientes_uti_ultimo_dia", round(regexp_replace(col("pacientes_uti_ultimo_dia"), ",", ".").cast("double"), 2)) \
    .withColumn("nr_covid_uti_ultimo_dia", round(regexp_replace(col("total_covid_uti_ultimo_dia"), ",", ".").cast("double"), 2)) \
    .withColumn("nr_ocupacao_leitos_ultimo_dia", round(regexp_replace(col("ocupacao_leitos_ultimo_dia"), ",", ".").cast("double"), 2)) \
    .withColumn("nr_internacoes_ultimo_dia", col("internacoes_ultimo_dia").cast("int")) \
    .withColumn("mm7d_pacientes_enf", round(regexp_replace(col("pacientes_enf_mm7d"), ",", ".").cast("double"), 2)) \
    .withColumn("mm7d_covid_enf", round(regexp_replace(col("total_covid_enf_mm7d"), ",", ".").cast("double"), 2)) \
    .withColumn("nr_pacientes_enf_ultimo_dia", round(regexp_replace(col("pacientes_enf_ultimo_dia"), ",", ".").cast("double"), 2)) \
    .withColumn("nr_covid_enf_ultimo_dia", round(regexp_replace(col("total_covid_enf_ultimo_dia"), ",", ".").cast("double"), 2)) \
    .drop("nm_arquivo") \
    .drop("dt_carga") \
    .drop("nome_drs") \
    .drop("data_hora") \
    .drop("pacientes_uti_mm7d") \
    .drop("total_covid_uti_mm7d") \
    .drop("ocupacao_leitos") \
    .drop("leitos_pc") \
    .drop("internacoes_7d") \
    .drop("internacoes_7d_l") \
    .drop("internacoes_7v7") \
    .drop("pacientes_uti_ultimo_dia") \
    .drop("ocupacao_leitos_ultimo_dia") \
    .drop("internacoes_ultimo_dia") \
    .drop("pacientes_enf_mm7d") \
    .drop("total_covid_enf_mm7d") \
    .drop("pacientes_enf_ultimo_dia") \
    .drop("total_covid_enf_ultimo_dia") \
    .drop("pop") \
    .drop("total_covid_uti_ultimo_dia")



display(df_prata_leitos_internacoes.limit(10)) 
 

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gravar transformação na camada prata

# COMMAND ----------

df_prata_leitos_internacoes.withColumn("Ano",year(col("dt_internacao"))) \
  .withColumn("Mes",month(col("dt_internacao"))) \
  .write.option("maxRecordsPerFile",50000) \
  .partitionBy("Ano","Mes") \
  .format("parquet") \
  .mode("overwrite") \
  .save(silver_leitos_internacoes_path)

# COMMAND ----------

df_prata_leitos_internacoes_processado = spark.read.format("parquet").load(silver_leitos_internacoes_path)
df_prata_leitos_internacoes_processado.select("nm_regiao").distinct().limit(10).show()

# COMMAND ----------

df_prata_leitos_internacoes_processado.select("nm_regiao").distinct().limit(200).show()

# COMMAND ----------


