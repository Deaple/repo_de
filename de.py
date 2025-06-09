json_schema = get_message_schema()

df_json = df.filter(col("msg_type") == "json") \
    .withColumn("parsed", from_json(col("message"), json_schema)).drop("parsed")

df_kv = df.filter(col("msg_type") == "kv") \
    .withColumn("parsed", col("message")).drop("parsed")

df_json.show(truncate=False)
df_kv.show(truncate=False)



from pyspark.sql.functions import regexp_extract

max_key = 30

for i in range(max_key + 1):
    pattern = r'%d:"([^"]*)"' % i
    df_kv = df_kv.withColumn(str(i), regexp_extract(col("message"), pattern, 1))

df_kv.select([str(i) for i in range(max_key + 1)]).show(truncate=False)
