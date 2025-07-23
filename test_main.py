import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("GlueTest") \
        .getOrCreate()

def test_filter_and_partition(spark):
    data = [
        ("SP", "SQUAD1", "RT1", "COM1", "DIR1", 202501),
        ("RJ", "SQUAD2", "RT2", "COM2", "DIR2", 202502),
        ("MG", "SQUAD3", "RT3", "COM3", "DIR3", 202503),
        ("BA", "SQUAD4", "RT4", "COM4", "DIR4", 202504)
    ]
    schema = "sigla STRING, squad STRING, release_train STRING, comunidade STRING, diretoria STRING, ano_mes INT"
    df = spark.createDataFrame(data, schema=schema)

    # Create Fridays DataFrame
    fridays_df = spark.sql("""
        SELECT explode(sequence(
            to_date('2025-01-03'), 
            to_date('2025-04-04'), 
            interval 1 week
        )) as friday_date
    """).withColumn("ano_mes_dia", col("friday_date").cast("date").cast("string").cast("int")) \
      .withColumn("ano_mes", col("friday_date").cast("string").substr(1, 6).cast("int")) \
      .drop("friday_date")

    # Join
    result_df = df.join(fridays_df, on="ano_mes", how="inner")

    assert result_df.count() == fridays_df.count(), "Row count mismatch with Fridays"
    assert "ano_mes_dia" in result_df.columns, "ano_mes_dia partition column missing"
    assert result_df.select("ano_mes").distinct().count() == 4, "Missing some months"

