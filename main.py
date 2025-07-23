import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit
from pyspark.sql.types import IntegerType
import datetime

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_PATH', 'DATABASE_NAME', 'TABLE_NAME'])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def get_fridays_in_range(start_date, end_date):
    """Generate all Fridays between two dates in YYYYMMDD format"""
    start = datetime.datetime.strptime(start_date, "%Y%m%d")
    end = datetime.datetime.strptime(end_date, "%Y%m%d")
    
    fridays = []
    current = start
    while current <= end:
        if current.weekday() == 4:  # Friday
            fridays.append(current.strftime("%Y%m%d"))
        current += datetime.timedelta(days=1)
    return fridays

def process_partition(df, ano_mes_dia):
    """Process data for a specific partition"""
    date = datetime.datetime.strptime(ano_mes_dia, "%Y%m%d")
    ano_mes = date.strftime("%Y%m")
    
    # Filter data for the corresponding month
    partition_data = df.filter(col("ano_mes") == int(ano_mes))
    
    # Add partition column
    return partition_data.withColumn("ano_mes_dia", lit(int(ano_mes_dia)))

# Read input CSV
input_df = spark.read.option("header", "true").csv(args['INPUT_PATH'])

# Convert ano_mes to integer
input_df = input_df.withColumn("ano_mes", col("ano_mes").cast(IntegerType()))

# Get all Fridays in date range
fridays = get_fridays_in_range("20250103", "20250404")

# Process each partition
for friday in fridays:
    partition_df = process_partition(input_df, friday)
    
    # Write to Glue catalog with overwrite partition
    partition_df.write.mode("overwrite").format("parquet") \
        .option("path", f"{args['DATABASE_NAME']}/{args['TABLE_NAME']}") \
        .partitionBy("ano_mes_dia") \
        .saveAsTable(f"{args['DATABASE_NAME']}.{args['TABLE_NAME']}", mode="overwrite")

job.commit()
