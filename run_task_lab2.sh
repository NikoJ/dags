#!/bin/bash

PYSPARK_PYTHON=python3 spark-submit \
    --conf spark.streaming.batch.duration=15 \
    --master local[1] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 \
    task_02.py