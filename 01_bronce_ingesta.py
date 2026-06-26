# Databricks notebook source
df_contratos = spark.table("workspace.default.contratos")

display(df_contratos)


# COMMAND ----------

# DBTITLE 1,Bronze Layer - Load raw data
# BRONZE LAYER: Load raw data as-is
from pyspark.sql.functions import current_timestamp

# Create bronze table with metadata
df_bronze = df_contratos.withColumn("ingestion_timestamp", current_timestamp())

# Write to Delta table
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_contratos")

print(f"Bronze table created with {df_bronze.count()} records")
display(df_bronze.limit(5))

# COMMAND ----------

# DBTITLE 1,Silver Layer - Clean and transform data
# SILVER LAYER: Clean and transform data
from pyspark.sql.functions import to_date, col

# Read from bronze
df_bronze_read = spark.table("workspace.default.bronze_contratos")

# Apply transformations
df_silver = df_bronze_read \
    .filter(col("id_contrato").isNotNull()) \
    .dropDuplicates(["id_contrato"]) \
    .withColumn("fecha_inicio", to_date(col("fecha_inicio"), "yyyy-MM-dd")) \
    .withColumn("fecha_fin", to_date(col("fecha_fin"), "yyyy-MM-dd")) \
    .withColumn("processing_timestamp", current_timestamp())

# Write to Delta table
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_contratos")

print(f"Silver table created with {df_silver.count()} records")
print("\nSchema after transformation:")
df_silver.printSchema()
display(df_silver.limit(5))

# COMMAND ----------

# DBTITLE 1,Gold Layer - KPIs and aggregations
# GOLD LAYER: Create business KPIs
from pyspark.sql.functions import count, sum as spark_sum, col

# Read from silver
df_silver_read = spark.table("workspace.default.silver_contratos")

# Calculate overall metrics
total_contratos = df_silver_read.count()
total_valor = df_silver_read.agg(spark_sum("valor_contrato").alias("total")).collect()[0]["total"]

# Aggregations by modality
df_by_modalidad = df_silver_read.groupBy("modalidad") \
    .agg(
        count("*").alias("cantidad_contratos"),
        spark_sum("valor_contrato").alias("valor_total")
    )

# Aggregations by state
df_by_estado = df_silver_read.groupBy("estado") \
    .agg(
        count("*").alias("cantidad_contratos"),
        spark_sum("valor_contrato").alias("valor_total")
    )

# Create unified gold table with all KPIs
from pyspark.sql.functions import lit

df_gold_kpis = df_by_modalidad.withColumn("dimension", lit("modalidad")) \
    .withColumnRenamed("modalidad", "categoria") \
    .union(
        df_by_estado.withColumn("dimension", lit("estado")) \
        .withColumnRenamed("estado", "categoria")
    ) \
    .withColumn("processing_timestamp", current_timestamp())

# Write to Delta table
df_gold_kpis.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_contratos_kpis")

print("="*80)
print("GOLD LAYER - KPIs SUMMARY")
print("="*80)
print(f"\nTotal Contratos: {total_contratos:,}")
print(f"Total Valor Contratos: ${total_valor:,.0f}")
print("\n" + "="*80)
print("Contratos por Modalidad:")
print("="*80)
display(df_by_modalidad.orderBy(col("cantidad_contratos").desc()))
print("\n" + "="*80)
print("Contratos por Estado:")
print("="*80)
display(df_by_estado.orderBy(col("cantidad_contratos").desc()))