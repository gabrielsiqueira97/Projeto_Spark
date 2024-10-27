# Databricks notebook source
# Criar o banco de dados
spark.sql("CREATE DATABASE IF NOT EXISTS LH_Gold")

# Usar o banco de dados
spark.sql("USE LH_Gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Criando tabelas Delta

# COMMAND ----------

delta_table_path = "dbfs:/LakeHouse/gold/covid/capacidade_testagem/fato_capacidade_testagem"
# Registre a tabela Delta no catálogo do Spark
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS LH_Gold.fato_capacidade_testagem
    USING DELTA
    LOCATION '{delta_table_path}'
""")

# Verifique se a tabela foi criada
spark.sql("SHOW TABLES").show()
