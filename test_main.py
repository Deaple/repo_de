import unittest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import main

# Initialize Spark session for testing
spark = SparkSession.builder.appName("UnitTest").getOrCreate()

def test_get_fridays_in_range():
    # Test with a known range
    start_date = "20250101"
    end_date = "20250131"
    fridays = main.get_fridays_in_range(start_date, end_date)
    
    # January 2025 has Fridays on 3rd, 10th, 17th, 24th, 31st
    expected = ["20250103", "20250110", "20250117", "20250124", "20250131"]
    assert fridays == expected, f"Expected {expected}, got {fridays}"
    
    # Test with a range that has no Fridays
    start_date = "20250101"
    end_date = "20250102"
    fridays = main.get_fridays_in_range(start_date, end_date)
    assert fridays == [], f"Expected empty list, got {fridays}"
    
    print("test_get_fridays_in_range passed")

def test_process_partition():
    # Create test data schema
    schema = StructType([
        StructField("sigla", StringType()),
        StructField("squad", StringType()),
        StructField("release_train", StringType()),
        StructField("comunidade", StringType()),
        StructField("diretoria", StringType()),
        StructField("ano_mes", IntegerType())
    ])
    
    # Create test data
    data = [
        ("A1", "Squad1", "RT1", "Com1", "Dir1", 202501),
        ("A2", "Squad2", "RT2", "Com2", "Dir2", 202501),
        ("B1", "Squad3", "RT3", "Com3", "Dir3", 202502)
    ]
    
    df = spark.createDataFrame(data, schema)
    
    # Test processing for a January date
    jan_date = "20250103"
    result = main.process_partition(df, jan_date)
    
    # Should only have January data (2 rows)
    assert result.count() == 2, f"Expected 2 rows, got {result.count()}"
    assert result.first()["ano_mes_dia"] == 20250103, "Incorrect partition value"
    
    # Test processing for a February date
    feb_date = "20250207"
    result = main.process_partition(df, feb_date)
    
    # Should only have February data (1 row)
    assert result.count() == 1, f"Expected 1 row, got {result.count()}"
    assert result.first()["ano_mes_dia"] == 20250207, "Incorrect partition value"
    
    print("test_process_partition passed")

def run_tests():
    test_get_fridays_in_range()
    test_process_partition()
    spark.stop()

if __name__ == "__main__":
    run_tests()
