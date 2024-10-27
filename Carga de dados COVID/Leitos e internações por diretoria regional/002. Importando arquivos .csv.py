# Databricks notebook source
import urllib.request

# URL do arquivo no GitHub
url = 'https://media.githubusercontent.com/media/gabrielsiqueira97/Projeto_Spark/refs/heads/main/Base%20de%20dados/20240121_leitos_internacoes.csv'
##6-Leitos e Internações por diretoria regional de saúde\n",
# Registro de leitos destinados a COVID-19 e internações (por COVID-19 ou suspeita de COVID-19) por Departamento Regional de Saúde desde o dia 18 de junho segundo os critérios utilizados no Plano SP.\n",
# https://www.saopaulo.sp.gov.br/planosp/simi/dados-abertos/ 
# Caminho temporário na instância do cluster
temp_path = '/tmp/20240121_leitos_internacoes.csv'

# Baixar o arquivo para o caminho temporário
urllib.request.urlretrieve(url, temp_path)

# Caminho de destino no DBFS
dbfs_path = '/LakeHouse/landingzone/covid/leitos_internacoes/processar'

# Mover o arquivo para o DBFS
dbutils.fs.cp(f'file:{temp_path}', f'dbfs:{dbfs_path}')

print(f"Arquivo baixado e salvo em: {dbfs_path}")

# COMMAND ----------

dbutils.fs.ls("/LakeHouse/landingzone/covid/isolamento/processar/")
