# Databricks notebook source
import urllib.request

# URL do arquivo no GitHub
url = 'https://media.githubusercontent.com/media/gabrielsiqueira97/Projeto_Spark/refs/heads/main/Base%20de%20dados/20220211_isolamento.csv'

# Caminho temporário na instância do cluster
temp_path = '/tmp/20220211_isolamento.csv'

# Baixar o arquivo para o caminho temporário
urllib.request.urlretrieve(url, temp_path)

# Caminho de destino no DBFS
dbfs_path = '/LakeHouse/landingzone/covid/isolamento/processar/dados_2012.csv'

# Mover o arquivo para o DBFS
dbutils.fs.cp(f'file:{temp_path}', f'dbfs:{dbfs_path}')

print(f"Arquivo baixado e salvo em: {dbfs_path}")

# COMMAND ----------

dbutils.fs.ls("/LakeHouse/landingzone/covid/isolamento/processar/")
