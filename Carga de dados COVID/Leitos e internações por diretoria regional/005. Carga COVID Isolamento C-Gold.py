# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

spark = SparkSession.builder \
    .appName("Carga Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------

silver_isolamento_path = "/LakeHouse/silver/covid/isolamento"
gold_isolamento_path = "/LakeHouse/gold/covid/isolamento"

# COMMAND ----------

df_silver_isolamento = spark.read.format("parquet").load(silver_isolamento_path)
display(df_silver_isolamento)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Criação da Dimensão municipio

# COMMAND ----------

tb_destino = "dim_municipio"
# Extrair Municipios únicos para a dimensão Municipios
df_dim_municipio = df_silver_isolamento.select("cd_municipio_ibge","ds_municipio","ds_uf","qtd_populacao_estimada").dropDuplicates()

#adicionando chave unica
df_dim_municipio = df_dim_municipio.withColumn("sk_municipio",monotonically_increasing_id()+1)

#escrevendo a dim_municipio em formato delta
df_dim_municipio.write.format("Delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_isolamento_path}/{tb_destino}")


display(df_dim_municipio)
df_dim_municipio.count()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Criação da FATO Isolamento

# COMMAND ----------

tb_destino_ft_isolamento = "fato_isolamento"
df_gold_fato_isolamento = df_silver_isolamento.select("pct_indice_isolamento","dt_pesquisa","ano","mes","cd_municipio_ibge","ds_municipio","ds_uf","qtd_populacao_estimada").dropDuplicates()

df_gold_fato_isolamento_sk = df_gold_fato_isolamento.alias("isolamento") \
  .join(df_dim_municipio.alias("municipio"),
        (col("isolamento.cd_municipio_ibge") == col("municipio.cd_municipio_ibge")) &
        (col("isolamento.ds_municipio") == col("municipio.ds_municipio")) &
        (col("isolamento.ds_uf") == col("municipio.ds_uf")) &
        (col("isolamento.qtd_populacao_estimada") == col("municipio.qtd_populacao_estimada")),
        "left") \
        .select("pct_indice_isolamento","dt_pesquisa","ano","mes","municipio.sk_municipio")

df_gold_fato_isolamento_sk = df_gold_fato_isolamento_sk.withColumn("sk_isolamento",monotonically_increasing_id()+1)


df_gold_fato_isolamento_sk.write.format("Delta") \
  .mode("overwrite") \
  .partitionBy("ano","mes") \
  .option("MaxRecordsPerFile", 50000) \
  .option("overwriteSchema", "true") \
  .save(f"{gold_isolamento_path}/{tb_destino_ft_isolamento}")


display(df_gold_fato_isolamento_sk)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS delta.`{gold_isolamento_path}/{tb_destino_ft_isolamento}`")

