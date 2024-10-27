# Databricks notebook source
#Criando diretorios no DBFS

dbutils.fs.mkdirs("/LakeHouse/landingzone/covid/leitos_internacoes/processado")
dbutils.fs.mkdirs("/LakeHouse/landingzone/covid/leitos_internacoes/processar")
dbutils.fs.mkdirs("/LakeHouse/bronze/covid/leitos_internacoes")

dbutils.fs.mkdirs("/LakeHouse/silver/covid/leitos_internacoes")
dbutils.fs.mkdirs("/LakeHouse/gold/covid/leitos_internacoes")





# COMMAND ----------

# Remover diretórios e seus conteúdos de forma recursiva
dbutils.fs.rm("LakeHouse/landingzone/covid/leitos_internacoes/processado", recurse=True)
dbutils.fs.rm("LakeHouse/landingzone/covid/leitos_internacoes/processar", recurse=True) 
dbutils.fs.rm("LakeHouse/bronze/covid/leitos_internacoes", recurse=True)
dbutils.fs.rm("LakeHouse/silver/covid/leitos_internacoes", recurse=True)
dbutils.fs.rm("LakeHouse/gold/covid/leitos_internacoes", recurse=True)


# COMMAND ----------

# MAGIC %fs ls 

# COMMAND ----------

# MAGIC %fs ls /LakeHouse/bronze/covid
# MAGIC

# COMMAND ----------

# MAGIC %fs ls /FileStore/
