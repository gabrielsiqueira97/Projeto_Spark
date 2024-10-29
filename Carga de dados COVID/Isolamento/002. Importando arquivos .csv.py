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

# COMMAND ----------

# Caminho do arquivo no DBFS
dbfs_file_path = '/Workspace/Users/gabriel.siqueira@einstein.br/Projeto_Spark/Base de dados/20220211_isolamento.csv'

# Caminho de destino no DBFS
destination_path = '/LakeHouse/landingzone/covid/isolamento/processar/dados_2012.csv'

# Verificar se o arquivo de destino já existe e removê-lo
try:
    dbutils.fs.rm(destination_path, recurse=True)  # Remove o arquivo de destino, se existir
except Exception as e:
    print(f"Erro ao remover o arquivo: {e}")

# Mover o arquivo para o DBFS
dbutils.fs.cp(dbfs_file_path, destination_path)

print(f"Arquivo movido e salvo em: {destination_path}")


# COMMAND ----------

# Listar todos os usuários
dbutils.fs.ls('dbfs:/')


