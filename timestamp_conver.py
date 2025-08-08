from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

def convert_to_timestamp(df, colname, new_colname=None):
    """convert datetime formats to timestamp snippet
    """
    if new_colname is None:
        new_colname = colname

    return df.withColumn(
        new_colname,
        F.coalesce(
            # ISO 8601 
            F.to_timestamp(colname, "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
            # Apache log
            F.to_timestamp(colname, "dd/MMM/yyyy:HH:mm:ss Z")
        )
    )
