# Databricks notebook source
#Criando diretorios no DBFS

dbutils.fs.mkdirs("/LakeHouse/landingzone/covid/isolamento/processado")
dbutils.fs.mkdirs("/LakeHouse/landingzone/covid/isolamento/processar")
dbutils.fs.mkdirs("/LakeHouse/bronze/covid/isolamento")

dbutils.fs.mkdirs("/LakeHouse/silver/covid/isolamento")
dbutils.fs.mkdirs("/LakeHouse/gold/covid/isolamento")





# COMMAND ----------

# Remover diretórios e seus conteúdos de forma recursiva
dbutils.fs.rm("LakeHouse/landingzone/covid/isolamento/processado", recurse=True)
dbutils.fs.rm("LakeHouse/landingzone/covid/isolamento/processar", recurse=True) 
dbutils.fs.rm("LakeHouse/bronze/covid/isolamento", recurse=True)
dbutils.fs.rm("LakeHouse/silver/covid/isolamento", recurse=True)
dbutils.fs.rm("LakeHouse/gold/covid/isolamento", recurse=True)


# COMMAND ----------

# MAGIC %fs ls 

# COMMAND ----------

# MAGIC %fs ls /base/LakeHouse
# MAGIC

# COMMAND ----------

# MAGIC %fs ls /FileStore/
