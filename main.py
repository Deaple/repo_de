import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, explode, sequence, to_date, date_format
from pyspark.sql.types import DateType

# Parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'input_path',
    'database_name',
    'table_name'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

input_path = args['input_path']
database = args['database_name']
table = args['table_name']

# Load CSV
df = spark.read.option("header", "true").csv(input_path)

# Convert ano_mes to date
df = df.withColumn("ano_mes", col("ano_mes").cast("int"))

# Create a DataFrame of Fridays from 2025-01-03 to 2025-04-04
fridays_df = spark.sql("""
    SELECT explode(sequence(
        to_date('2025-01-03'), 
        to_date('2025-04-04'), 
        interval 1 week
    )) as friday_date
""").withColumn("ano_mes_dia", date_format(col("friday_date"), "yyyyMMdd").cast("int")) \
  .withColumn("ano_mes", date_format(col("friday_date"), "yyyyMM").cast("int")) \
  .drop("friday_date")

# Join with original data on ano_mes
final_df = df.join(fridays_df, on="ano_mes", how="inner")

# Write to Glue table with overwrite dynamic partition mode
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

final_df.write.mode("overwrite") \
    .partitionBy("ano_mes_dia") \
    .format("parquet") \
    .saveAsTable(f"{database}.{table}")
