# Databricks notebook source
#Criando diretorios no DBFS

dbutils.fs.mkdirs("/LakeHouse/landingzone/covid/capacidade_testagem/processado")
dbutils.fs.mkdirs("/LakeHouse/landingzone/covid/capacidade_testagem/processar")
dbutils.fs.mkdirs("/LakeHouse/bronze/covid/capacidade_testagem")

dbutils.fs.mkdirs("/LakeHouse/silver/covid/capacidade_testagem/parquet")
dbutils.fs.mkdirs("/LakeHouse/silver/covid/capacidade_testagem/delta")
dbutils.fs.mkdirs("/LakeHouse/gold/covid/capacidade_testagem")





# COMMAND ----------

# Remover diretórios e seus conteúdos de forma recursiva
dbutils.fs.rm("LakeHouse/landingzone/covid/capacidade_testagem/processado", recurse=True)
dbutils.fs.rm("LakeHouse/landingzone/covid/capacidade_testagem/processar", recurse=True) 
dbutils.fs.rm("LakeHouse/bronze/covid/capacidade_testagem", recurse=True)
dbutils.fs.rm("LakeHouse/silver/covid/capacidade_testagem/parquet", recurse=True)
dbutils.fs.rm("LakeHouse/silver/covid/capacidade_testagem/delta", recurse=True)
dbutils.fs.rm("LakeHouse/gold/covid/capacidade_testagem", recurse=True)


# COMMAND ----------

# MAGIC %fs ls 

# COMMAND ----------

# MAGIC %fs ls /base/LakeHouse
# MAGIC

# COMMAND ----------

# MAGIC %fs ls /FileStore/
