from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.dataframe import *
from pyspark.sql import SparkSession
import pyspark
import pytest
from time import time
from uuid import uuid4
from chispa.dataframe_comparer import *
from datetime import datetime
from app import add_ingestion_tms_and_uuid_v4, append_dataframe_to_delta_table

data_schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("location_id", StringType(), False),
        StructField("date", TimestampType(), False),
        StructField("price", FloatType(), False),
        StructField("cost", FloatType(), False),
    ])

@pytest.fixture
def spark():
    """ Build and return a spark session to use across tests"""
    builder = pyspark.sql.SparkSession.builder.appName("LocalTests") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.session.timeZone", "America/Los_Angeles")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()


def test_ingress_csv_file_with_header(spark) -> None:
    """Read a csv file into a DataFrame"""
    data = spark.read.csv(
        path="resources/header-table.csv", 
        schema=data_schema,
        header=True,
        sep="|"
    )

    assert data.count() == 12


def test_ingress_csv_file_without_header(spark) -> None:
    """Read a headerless csv file into a DataFrame"""
    data = spark.read.csv(
        path="resources/headerless-table.csv", 
        schema=data_schema,
        header=False,
        sep="|"
    )

    assert data.count() == 12


def test_add_ingestion_tms_and_uuid_v4(spark) -> None:
    """Read a headerless csv file into a DataFrame"""
    input_df = spark.createDataFrame([(1230219000,)], ['col_a'])
    enriched_df = add_ingestion_tms_and_uuid_v4(
        input_df=input_df, 
        ts=1709393455,
        batch_id="1709393455"
    )

    ingestion_tms_schema = StructType([
        StructField("ingestion_tms",TimestampType(),False),
        StructField("count",LongType(),False),
    ])
    batch_id_schema = StructType([
        StructField("batch_id",StringType(),False),
        StructField("count",LongType(),False),
    ])
    ingestion_tms_expected_count_df = spark.createDataFrame(
        data=[(datetime(2024,3,2,16,30,55),1)], 
        schema=ingestion_tms_schema
    )
    batch_id_expected_count_df = spark.createDataFrame(
        data=[("1709393455",1)], 
        schema=batch_id_schema
    )

    assert len(enriched_df.columns) == len(input_df.columns) + 2
    assert_df_equality(
        enriched_df.groupBy("ingestion_tms").count(),
        ingestion_tms_expected_count_df
    )
    assert_df_equality(
        enriched_df.groupBy("batch_id").count(),
        batch_id_expected_count_df
    )


def test_add_ingestion_tms_and_uuid_v4(spark) -> None:
    """
    Enrich a dataframe with ingestion timestamp and UUIDv4
    Example
    >>> input_df = spark.createDataFrame([(1230219000,)], ['col_a'])
    >>> enriched_df = add_ingestion_tms_and_uuid_v4(input_df)
    >>> enriched_df.show()
    +----------+-------------------+------------------------------------+
    |col_a     |ingestion_tms      |batch_id                            |
    +----------+-------------------+------------------------------------+
    |1230219000|2024-03-02 09:03:08|7da16658-1b14-4d93-a491-f78be0aca95b|
    +----------+-------------------+------------------------------------+
    """
    ingestion_tms_schema = StructType([
        StructField("ingestion_tms",TimestampType(),False),
        StructField("count",LongType(),False),
    ])
    batch_id_schema = StructType([
        StructField("batch_id",StringType(),False),
        StructField("count",LongType(),False),
    ])
    ingestion_tms_expected_count_df = spark.createDataFrame(
        data=[(datetime(2024,3,2,16,30,55),1)], 
        schema=ingestion_tms_schema
    )
    batch_id_expected_count_df = spark.createDataFrame(
        data=[("1709393455",1)], 
        schema=batch_id_schema
    )
    input_df = spark.createDataFrame(
        data=[("delta table rocks!",)], 
        schema=StructType([
        StructField("col_a",StringType(),True)
        ])
    )

    enriched_df = add_ingestion_tms_and_uuid_v4(
        input_df=input_df, 
        ts=1709393455,
        batch_id="1709393455"
    )

    assert len(enriched_df.columns) == len(input_df.columns) + 2
    assert_df_equality(
        enriched_df.groupBy("ingestion_tms").count(),
        ingestion_tms_expected_count_df
    )
    assert_df_equality(
        enriched_df.groupBy("batch_id").count(),
        batch_id_expected_count_df
    )


def test_ingest_one_file_and_append_to_delta_table(spark) -> None:
    """
    Ingest csv files into dataframes and merge them into a single delta-table using a specific key
    """ 
    data = spark.read.csv(
        path="resources/header-table.csv", 
        schema=data_schema,
        header=True,
        sep="|"
    )
    data \
        .write \
        .format("delta") \
        .mode("overwrite") \
        .save("/tmp/data-table")

    append_dataframe_to_delta_table(
        spark_session=spark,
        delta_file_path="resources/headerless-table-updates.csv",
        delta_table_path="/tmp/data-table",
        data_schema=data_schema,
        csv_sep="|",
        delta_table_keys_list=["product_id", "location_id", "date"]
    )
    
    number_written_records = spark \
        .read \
        .format("delta") \
        .load("/tmp/data-table") \
        .count()
    
    assert number_written_records == 13
