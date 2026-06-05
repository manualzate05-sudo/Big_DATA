from pyspark import pipelines as dp

@dp.materialized_view(
    name="bronze_eficiencia_energetica",
    comment="Bronze layer: Raw energy efficiency data from Aragón buildings"
)
def bronze_eficiencia_energetica():
    """
    Bronze layer - reads raw data from workspace.bronze.eficiencia_energetica_aragon
    No transformations, just ingesting the data as-is into the pipeline
    """
    return spark.read.table("workspace.bronze.eficiencia_energetica_aragon")
