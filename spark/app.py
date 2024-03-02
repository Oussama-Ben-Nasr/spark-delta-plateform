from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.dataframe import *
from pyspark.sql import SparkSession
from time import time
from uuid import uuid4
from chispa.dataframe_comparer import *

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

def append_dataframe_to_delta_table(spark_session:SparkSession, delta_file_path:str, delta_table_path:str, data_schema:StructType, csv_sep:str, delta_table_keys_list:list[str]) -> None:
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
  print("Entry point")

if __name__ == '__main__':
  main()