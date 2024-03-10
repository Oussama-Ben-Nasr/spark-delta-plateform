from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.dataframe import *
from pyspark.sql import SparkSession
import pyspark
from uuid import uuid4
from chispa.dataframe_comparer import *
from typing import List
from time import time


def add_ingestion_tms_and_uuid_v4(input_df: DataFrame, **kwargs) -> DataFrame:
    """
    A function that enriches dataframe with additional two columns
        * ingestion_tms: timestamp in the format YYYY-MM-DD HH:mm:SS
        * batch_id: a uuid v4
    Internally no arguments shall be provided so that we can have deterministic output.
    """
    ts = kwargs.get('ts', int(time()))
    batch_id = kwargs.get('batch_id', str(uuid4()))

    enriched_df = input_df \
        .withColumn("ingestion_tms", timestamp_seconds(lit(ts)))\
        .withColumn("batch_id", lit(batch_id))
    return enriched_df


def append_dataframe_to_delta_table(spark_session: SparkSession, delta_file_path: str, delta_table_path: str, data_schema: StructType, csv_sep: str, delta_table_keys_list: List[str]) -> None:
    data = DeltaTable.forPath(spark_session, delta_table_path)
    delta = spark_session.read.csv(
        path=delta_file_path,
        schema=data_schema,
        header=False,
        sep=csv_sep
    )
    data.alias("data") \
        .merge(
        delta.alias("delta"),
        reduce(
            str.__add__,
            [
                f"data.{key_field_name} = delta.{key_field_name} AND "
                for key_field_name in delta_table_keys_list
            ],
            str()
        )
    ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()


def main() -> None:
    builder = pyspark.sql.SparkSession.builder\
        .master("spark://spark-master:7077").appName("LocalCluster") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "/opt/workspace/history") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
 
    # Prepare client data, 
    # In real world df can be read from 
    # a remote blob storage service (Amazon, Azuere, GCP) 
    schema = StructType([
        StructField("key", StringType(), False),
        StructField("value", LongType(), False),
    ])
    enriched_schema = StructType([
        StructField("key", StringType(), False),
        StructField("value", LongType(), False),
        StructField("ingestion_tms", TimestampType(), False),
        StructField("batch_id", StringType(), False),
    ])
    df = spark.createDataFrame(
        data=[("secret of life", 42)],
        schema=schema
    )
    df.show()

    # Enrich client data with ingestion time and uuid
    # And persist that internally in adelta table
    enriched_df = add_ingestion_tms_and_uuid_v4(df)
    enriched_df.show()
    enriched_df.write.format("delta").mode("overwrite").save("/opt/workspace/delta-tables-repo/table-one")

    # Next day the client sent us a csv file to be appended to the delta table, it was the same file :)
    # We did the same process and enriched that chunck of data and we wrote it to internally
    add_ingestion_tms_and_uuid_v4(
            spark.createDataFrame(
                data=[
                        ("secret of life", 41),
                        ("spark ratings", 100),
                ],
                schema=schema
            )
    ).write.mode("overwrite").csv("/opt/workspace/ingestion-area/delta.csv")

    # Append the file to our delta table 
    append_dataframe_to_delta_table(
            spark_session=spark, 
            delta_table_path="/opt/workspace/delta-tables-repo/table-one",
            delta_file_path="/opt/workspace/ingestion-area/delta.csv",
            data_schema=enriched_schema, 
            csv_sep=",",
            delta_table_keys_list=["key"]
    )

    # Show final result.
    DeltaTable.forPath(spark, "/opt/workspace/delta-tables-repo/table-one").toDF().show()

if __name__ == '__main__':
    main()
