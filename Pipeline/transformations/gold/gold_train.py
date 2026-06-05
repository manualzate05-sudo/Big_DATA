from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="gold_eficiencia_train",
    comment="Gold layer: Training dataset (70%) for ML modeling"
)
def gold_eficiencia_train():
    """
    Gold layer - Training split (70%)
    Uses hash-based splitting for reproducibility
    Hash values 0-69 (70% of 0-99 range)
    """
    df = spark.read.table("silver_eficiencia_energetica")
    
    # Create a unique row ID by combining multiple fields
    row_id = F.concat_ws("_", 
        F.col("Municipio"), 
        F.col("Coordenadas_gps"), 
        F.col("Anio_construccion").cast("string"),
        F.col("Superficie_m2").cast("string")
    )
    
    return (df
        # Add hash-based split column
        .withColumn("split_hash", F.abs(F.hash(row_id)) % 100)
        
        # Select training set (70%)
        .filter(F.col("split_hash") < 70)
        
        # Add split identifier
        .withColumn("split_type", F.lit("train"))
        
        # Drop the hash column
        .drop("split_hash")
    )
