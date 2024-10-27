# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark = SparkSession.builder \
    .appName("Carga de dados capacidade de isolamento camada Bronze") \
    .config("spark.sql.shuffle.partitions","4") \
    .config("spark.sql.files.maxPartitionBytes","128MB") \
    .config("spark.sql.parquet.compression.codec","snappy") \
    .config("spark.sql.adaptive.enabled","true") \
    .getOrCreate()



# Definir caminhos de armazenamento no Data Lake
lz_path_in = "/LakeHouse/landingzone/covid/capacidade_testagem/processar"
lz_path_out = "/LakeHouse/landingzone/covid/capacidade_testagem/processado"
bronze_path = "/LakeHouse/bronze/covid/capacidade_testagem"

# COMMAND ----------


# Definir o esquema
schema_lz_capacidade_testagem = StructType([
    StructField("nm_laboratorio", StringType(), True),
    StructField("vl_capacidade_nominal", IntegerType(), True)
])

# Ler o arquivo Excel
df_capacidade_testagem = spark.read.option("header", "true") \
    .option("delimiter", ";") \
    .option("encoding","UTF-8") \
    .schema(schema_lz_capacidade_testagem) \
    .csv(lz_path_in) \
    .withColumn("nm_arquivo", regexp_extract(input_file_name(), "([^/]+)$", 0)) \
    .withColumn("dt_carga", current_date())



display(df_capacidade_testagem)

# COMMAND ----------

distinct_filenames = df_capacidade_testagem.select("nm_arquivo").distinct()
df_capacidade_testagem.write.mode("overwrite").parquet(bronze_path)

# COMMAND ----------


#Movendo arquivo da lz_path_in para out
distinct_filenames.unpersist()
if distinct_filenames.select("nm_arquivo").distinct().count() > 0:
    filenames = distinct_filenames.select("nm_arquivo").distinct().collect()

    for row in filenames:
        src_path = row.nm_arquivo
        dbutils.fs.mv(lz_path_in + "/" + src_path, lz_path_out)

# COMMAND ----------

display(dbutils.fs.ls("/LakeHouse/landingzone/covid/capacidade_testagem/processar"))

