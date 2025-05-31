import os.path

import pytest
from pyspark.sql import SparkSession
from src.orders import load_dataset
from pyspark.sql.types import *
from pyspark.sql.functions import *

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("PySpark Pytest Example") \
        .master("local[*]") \
        .getOrCreate()
    print("SparkSession started.")
    yield spark
    spark.stop()
    print("SparkSession stopped.")

def test_corrupt_record(spark):
    data=[("OFF-LA-10003148","Office Supplies","Labels","Avery 51","Illinois",5.04,"OFF-LA-10003148,Office Supplies,Labels,Avery 51,Illinois,5.04,dfas")]
    df = load_dataset(spark, [os.path.join(os.path.dirname(__file__), 'resource/test_product.csv')])
    bdf = df.filter(col("_corrupt_record").isNotNull())
    res = [tuple(row) for row in bdf.collect()]
    res = [row[:-1] for row in res]
    assert res[0]==data[0]

def test_product_bad_count(spark):
    df = load_dataset(spark,[os.path.join(os.path.dirname(__file__),'resource/test_product.csv')])
    assert df.count() == 5
    bdf = df.filter(col("_corrupt_record").isNotNull()).collect()
    assert len(bdf) == 2


def test_order(spark):
    df = load_dataset(spark, [os.path.join(os.path.dirname(__file__), 'resource/test_Orders.json')])
    df.show(truncate=False)
    assert df.count() == 2


def test_product_schema(spark):
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("state", StringType(), True),
        StructField("price_per_product", DoubleType(), True),
        StructField('_corrupt_record', StringType(), True),
        StructField("load_ts", TimestampType(), False)
    ])
    df = spark.createDataFrame([],schema=schema)
    gdf = load_dataset(spark,[os.path.join(os.path.dirname(__file__),'resource/test_product.csv')])
    gdf.cache()
    assert df.schema == gdf.schema