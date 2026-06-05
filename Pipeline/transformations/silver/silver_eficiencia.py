from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="silver_eficiencia_energetica",
    comment="Silver layer: Cleaned and transformed energy efficiency data"
)
@dp.expect_all({
    "valid_emision_co2": "Emision_CO2 >= 0",
    "valid_consumo": "`ConsumoKWh/m2/Anio` >= 0",
    "valid_superficie": "Superficie_m2 > 0",
    "valid_anio_construccion": "Anio_construccion >= 1800 AND Anio_construccion <= year(current_date())"
})
def silver_eficiencia_energetica():
    """
    Silver layer - cleans and standardizes the bronze data:
    - Parse dates properly
    - Handle nulls
    - Standardize text fields
    - Add derived features
    - Apply data quality constraints
    """
    df = spark.read.table("bronze_eficiencia_energetica")
    
    return (df
        # Parse dates to proper date types
        .withColumn("fecha_emision_date", F.to_date("Fecha_emision", "yyyy-MM-dd"))
        .withColumn("fecha_expiracion_date", F.to_date("Fecha_expiracion", "yyyy-MM-dd"))
        
        # Clean and standardize text fields
        .withColumn("tipo_edificio_clean", F.trim(F.upper("Tipo_edificio")))
        .withColumn("estado_edificio_clean", F.trim(F.upper("Estado_edificio")))
        .withColumn("clasificacion_emisiones_clean", F.trim(F.upper("Clasificacion_Emisiones")))
        .withColumn("clasificacion_consumo_clean", F.trim(F.upper("Clasificacion_consumo")))
        .withColumn("municipio_clean", F.trim(F.upper("Municipio")))
        .withColumn("provincia_clean", F.trim(F.upper("Provincia")))
        
        # Add derived features
        .withColumn("edad_edificio", F.col("Anio_emision") - F.col("Anio_construccion"))
        .withColumn("consumo_total_kwh", F.col("`ConsumoKWh/m2/Anio`") * F.col("Superficie_m2"))
        .withColumn("emision_total_co2", F.col("Emision_CO2") * F.col("Superficie_m2"))
        
        # Parse GPS coordinates (format: "X , Y")
        .withColumn("coord_parts", F.split(F.col("Coordenadas_gps"), ","))
        .withColumn("gps_x", F.trim(F.col("coord_parts")[0]).cast("double"))
        .withColumn("gps_y", F.trim(F.col("coord_parts")[1]).cast("double"))
        .drop("coord_parts")
        
        # Add efficiency score (lower is better, combining both metrics)
        .withColumn("eficiencia_score", 
            (F.col("Emision_CO2") / 100.0 + F.col("`ConsumoKWh/m2/Anio`") / 500.0) / 2.0
        )
        
        # Keep original columns and add cleaned/derived ones
        .select(
            # Original columns
            "Fecha_emision", "Fecha_expiracion", "Emision_CO2", "Clasificacion_Emisiones",
            "`ConsumoKWh/m2/Anio`", "Clasificacion_consumo", "Tipo_edificio", "Estado_edificio",
            "Anio_construccion", "Superficie_m2", "Municipio", "Provincia", "Coordenadas_gps",
            "Anio_emision", "Dias_hasta_expiracion",
            # Parsed dates
            "fecha_emision_date", "fecha_expiracion_date",
            # Cleaned text
            "tipo_edificio_clean", "estado_edificio_clean", "clasificacion_emisiones_clean",
            "clasificacion_consumo_clean", "municipio_clean", "provincia_clean",
            # Derived features
            "edad_edificio", "consumo_total_kwh", "emision_total_co2",
            "gps_x", "gps_y", "eficiencia_score"
        )
    )
