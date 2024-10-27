# Databricks notebook source
# Criar o banco de dados
spark.sql("CREATE DATABASE IF NOT EXISTS LH_Gold")

# Usar o banco de dados
spark.sql("USE LH_Gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Criando tabelas Delta

# COMMAND ----------

delta_table_path = "dbfs:/LakeHouse/gold/covid/isolamento/dim_municipio"
# Registre a tabela Delta no catálogo do Spark
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS LH_Gold.dim_municipio
    USING DELTA
    LOCATION '{delta_table_path}'
""")

# Verifique se a tabela foi criada
spark.sql("SHOW TABLES").show()

# COMMAND ----------

delta_table_path = "dbfs:/LakeHouse/gold/covid/isolamento/fato_isolamento"
# Registre a tabela Delta no catálogo do Spark
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS LH_Gold.fato_isolamento
    USING DELTA
    LOCATION '{delta_table_path}'
""")

# Verifique se a tabela foi criada
spark.sql("SHOW TABLES").show()
