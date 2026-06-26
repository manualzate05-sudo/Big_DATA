# Databricks notebook source
from pyspark.sql.functions import *

# Leer tablas
df_contratos = spark.table("workspace.default.silver_contratos")
df_operadores = spark.table("workspace.default.operadores")
df_regionales = spark.table("workspace.default.regionales")

# Unir contratos con operadores
df_modelo = (
    df_contratos
    .join(df_operadores, on="operador_id", how="left")
)

display(df_modelo)

# COMMAND ----------

df_modelo.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.modelo_contratos")

print("Tabla modelo_contratos creada correctamente")

# COMMAND ----------

display(spark.table("workspace.default.modelo_contratos"))


# COMMAND ----------

# DBTITLE 1,Top 5 Modalities by Contract Count
# Top 5 modalities by number of contracts
top_modalities = (
    spark.table("workspace.default.modelo_contratos")
    .groupBy("modalidad")
    .count()
    .orderBy(col("count").desc())
    .limit(5)
)

display(top_modalities)

# COMMAND ----------

# DBTITLE 1,Contracts by State
# Count contracts by state
contracts_by_state = (
    spark.table("workspace.default.modelo_contratos")
    .groupBy("estado")
    .count()
    .orderBy(col("count").desc())
)

display(contracts_by_state)

# COMMAND ----------

# DBTITLE 1,Operator with Highest Total Contract Value
# Find operator with highest total contract value
top_operator = (
    spark.table("workspace.default.modelo_contratos")
    .groupBy("operador_id", "nombre", "nit")
    .agg(sum("valor_contrato").alias("total_valor"))
    .orderBy(col("total_valor").desc())
    .limit(10)
)

display(top_operator)

# COMMAND ----------

# Executive Dashboard for Contract Analysis
from pyspark.sql.functions import col, sum as spark_sum

# Load main table
df = spark.table("workspace.default.modelo_contratos")

# Total number of contracts (KPI)
total_contracts = df.count()
dbutils.widgets.text("Total Contracts", str(total_contracts))
displayHTML(f"<h2>Total Contracts: {total_contracts}</h2>")

# Total contract value (KPI)
total_value = df.agg(spark_sum("valor_contrato")).first()[0]
dbutils.widgets.text("Total Contract Value", str(total_value))
displayHTML(f"<h2>Total Contract Value: {total_value:,.0f}</h2>")

# Contracts by modality (bar chart)
contracts_by_modality = (
    df.groupBy("modalidad")
    .count()
    .orderBy(col("count").desc())
)
display(contracts_by_modality)

# Contracts by status (bar chart)
contracts_by_status = (
    df.groupBy("estado")
    .count()
    .orderBy(col("count").desc())
)
display(contracts_by_status)

# Total contract value by modality (bar chart)
value_by_modality = (
    df.groupBy("modalidad")
    .agg(spark_sum("valor_contrato").alias("total_valor"))
    .orderBy(col("total_valor").desc())
)
display(value_by_modality)

# Top 10 operators by total contract value (bar chart)
top_operators = (
    df.groupBy("operador_id", "nombre")
    .agg(spark_sum("valor_contrato").alias("total_valor"))
    .orderBy(col("total_valor").desc())
    .limit(10)
)
display(top_operators)