from delta import *
from pyspark.sql.functions import *
import pyspark
from time import sleep
from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.dataframe import *
import pyspark
import pytest
from time import time
from uuid import uuid4

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# Create table and save it
data = spark.range(0, 5)
data.write.format("delta").mode("overwrite").save("/tmp/delta-table")


# Read table from local file system
df = spark.read.format("delta").load("/tmp/delta-table")
df.show()


## Conditional update without overwrite
deltaTable = DeltaTable.forPath(spark, "/tmp/delta-table")

# Update every even value by adding 100 to it
deltaTable.update(
  condition = expr("id % 2 == 0"),
  set = { "id": expr("id + 100") })

# Delete every even value
deltaTable.delete(condition = expr("id % 2 == 0"))

# Upsert (merge) new data
newData = spark.range(0, 20)

deltaTable.alias("oldData") \
  .merge(
    newData.alias("newData"),
    "oldData.id = newData.id") \
  .whenMatchedUpdate(set = { "id": col("newData.id") }) \
  .whenNotMatchedInsert(values = { "id": col("newData.id") }) \
  .execute()

deltaTable.toDF().show()


## Read older version of data
df = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta-table")
df.show()


### Streaming

streamingDf = spark.readStream.format("rate").load()
stream = streamingDf.selectExpr("value as id").writeStream.format("delta").option("checkpointLocation", "/tmp/checkpoint").start("/tmp/delta-table")

# Wait for some time, then stop the stream
sleep(60)
stream.stop()