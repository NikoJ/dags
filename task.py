#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import functions as F
from pyspark.sql.types import *

from pyspark.sql.functions import explode
from pyspark.sql.functions import split

from pyspark.sql.functions import from_json


topic_in = "nikolay_potapov"
topic_out = topic_in + "_lab02_tutorial_out"
kafka_bootstrap = "10.0.10.9:6667"

spark = SparkSession.builder.appName("SimpleStreamingApp").getOrCreate()
spark.sparkContext.setLogLevel('WARN')


schema = StructType(
    fields = [
        StructField("timestamp", LongType(), True),
        StructField("referer", StringType(), True),
        StructField("location", StringType(), True),
        StructField("remoteHost", StringType(), True),
        StructField("partyId", StringType(), True),
        StructField("sessionId", StringType(), True),
        StructField("pageViewId", StringType(), True),
        StructField("eventType", StringType(), True),
        StructField("item_id", StringType(), True),
        StructField("item_price",IntegerType(), True),
        StructField("item_url", StringType(), True),
        StructField("basket_price",StringType(), True),
        StructField("detectedDuplicate", BooleanType(), True),
        StructField("detectedCorruption", BooleanType(), True),
        StructField("firstInSession", BooleanType(), True),
        StructField("userAgentName", StringType(), True),
    ])

## Считываем и распаковываем json-сообщения
st = spark \
    .readStream \
    .format("kafka") \
    .option("checkpointLocation", "/tmp/checkpoint-read") \
    .option("kafka.bootstrap.servers", kafka_bootstrap ) \
    .option("subscribe", topic_in) \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value as string)") \
    .select(from_json("value", schema).alias("value")) \
    .select(F.col("value.*")) \
 \
 \
## Формируем выходной датафрейм.
out_df = st

out_columns = list(out_df.columns)

query = out_df \
    .select(F.to_json(F.struct(*out_columns)).alias("value")) \
    .writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("checkpointLocation", "/tmp/checkpoint-write") \
    .option("kafka.bootstrap.servers", kafka_bootstrap ) \
    .option("topic", topic_out) \
    .start()

query.awaitTermination()