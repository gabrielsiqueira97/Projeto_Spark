# Databricks notebook source
# Criar o banco de dados
spark.sql("CREATE DATABASE IF NOT EXISTS LH_Silver")

# Usar o banco de dados
spark.sql("USE LH_Silver")

parquet_table_path = "dbfs:/LakeHouse/silver/covid/leitos_internacoes"
# Registre a tabela Delta no catálogo do Spark
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS LH_Silver.tb_leitos_internacoes
    USING PARQUET
    LOCATION '{parquet_table_path}'
""")

# Verifique se a tabela foi criada
spark.sql("SHOW TABLES").show()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Criando tabelas Delta

# COMMAND ----------

# Criar o banco de dados
spark.sql("CREATE DATABASE IF NOT EXISTS LH_Gold")

# Usar o banco de dados
spark.sql("USE LH_Gold")

delta_table_path = "dbfs:/LakeHouse/gold/covid/leitos_internacoes/fato_leitos_internacoes"
# Registre a tabela Delta no catálogo do Spark
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS LH_Gold.tb_leitos_internacoes
    USING DELTA
    LOCATION '{delta_table_path}'
""")

# Verifique se a tabela foi criada
spark.sql("SHOW TABLES").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct nm_regiao from lh_gold.tb_leitos_internacoes
