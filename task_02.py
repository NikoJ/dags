#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import functions as F
from pyspark.sql.types import *

from pyspark.sql.functions import explode
from pyspark.sql.functions import split

from pyspark.sql.functions import from_json
from pyspark.sql.functions import col

topic_in = "nikolay_potapov"
topic_out = topic_in + "_lab02_out"
kafka_bootstrap = "10.0.10.9:6667"

spark = SparkSession.builder.appName("Lab02. Lambda-архитектура. Speed Layer").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema_in = StructType([
    StructField("basket_price", StringType(), True),
    StructField("detectedCorruption", BooleanType(), True),
    StructField("detectedDuplicate", BooleanType(), True),
    StructField("eventType", StringType(), True),
    StructField("firstInSession", BooleanType(), True),
    StructField("item_id", StringType(), True),
    StructField("item_price", DoubleType(), True),
    StructField("item_url", StringType(), True),
    StructField("location", StringType(), True),
    StructField("pageViewId", StringType(), True),
    StructField("partyId", StringType(), True),
    StructField("referer", StringType(), True),
    StructField("remoteHost", StringType(), True),
    StructField("sessionId", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("userAgentName", StringType(), True)
])

## Считываем и распаковываем json-сообщения
st = spark \
    .readStream \
    .format("kafka") \
    .option("checkpointLocation", "/user/ubuntu/tmp/chk/checkpoint-read") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", topic_in) \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value as string)") \
    .select(from_json("value", schema_in).alias("value")) \
    .select(col("value.*")) \

st.printSchema()

## Формируем выходной датафрейм.
out_df = st

prepareDF = out_df \
    .filter((col("detectedCorruption") == False) & (col("detectedDuplicate") == False)) \
    .groupBy(F.window(F.to_timestamp(col("timestamp") / 1000), "10 minutes")) \
    .agg(F.sum(F.when(col("eventType") == "itemBuyEvent", col("item_price"))).alias("revenue"),
         F.approx_count_distinct(col("partyId")).alias("visitors"),
         F.approx_count_distinct(F.when(col("eventType") == "itemBuyEvent", col("sessionId"))).alias("purchases"))

prepareDF.printSchema()

resultDF = prepareDF \
    .select(F.unix_timestamp(prepareDF.window.start).alias("start_ts"),
            F.unix_timestamp(prepareDF.window.end).alias("end_ts"),
            "revenue",
            "visitors",
            "purchases",
            (col("revenue").cast("double") / col("purchases").cast("double")).alias("aov"))

resultDF.printSchema()

out_columns = list(resultDF.columns)

# debug
# .outputMode("complete") \
# .format("console") \
# .start() \

query = resultDF \
    .select(F.to_json(F.struct(*out_columns)).alias("value")) \
    .writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("checkpointLocation", "/user/ubuntu/tmp/chk/checkpoint-write") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("topic", topic_out) \
    .start()

query.awaitTermination()
