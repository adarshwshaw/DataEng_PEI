import os.path

import pytest
from pyspark.sql import SparkSession
from src.orders import load_dataset,cleanSchema
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

def test_product_bad_count(spark):
    df = load_dataset(spark,[os.path.join(os.path.dirname(__file__),'resource/test_Orders_corrupt.json')])
    assert df.count() == 1
    bdf = df.filter(col("_corrupt_record").isNotNull()).collect()
    assert len(bdf) == 1


def test_order(spark):
    df = load_dataset(spark, [os.path.join(os.path.dirname(__file__), 'resource/test_Orders.json')])
    df.show(truncate=False)
    assert df.count() == 2


def test_product_schema(spark):
    schema = StructType([
        StructField("Row ID", LongType(), True),
        StructField("Order ID", StringType(), True),
        StructField("Order Date", StringType(), True),
        StructField("Ship Date", StringType(), True),
        StructField("Ship Mode", StringType(), True),
        StructField("Customer ID", StringType(), True),
        StructField("Product ID", StringType(), True),
        StructField("Quantity", LongType(), True),
        StructField("Price", DoubleType(), True),
        StructField("Discount", DoubleType(), True),
        StructField("Profit", DoubleType(), True),
        StructField("_corrupt_record", StringType(), True),
        StructField('load_ts', TimestampType(), False)
    ])
    df = spark.createDataFrame([],schema=schema)
    gdf = load_dataset(spark,[os.path.join(os.path.dirname(__file__),'resource/test_product.csv')])
    assert df.schema == gdf.schema

def test_cleanSchema(spark):
    schema = StructType([
        StructField("row_id", LongType(), True),
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("ship_date", StringType(), True),
        StructField("ship_mode", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", LongType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("profit", DoubleType(), True),
        StructField("_corrupt_record", StringType(), True),
        StructField('load_ts', TimestampType(), False)
    ])
    df = spark.createDataFrame([], schema=schema)
    gdf = load_dataset(spark, [os.path.join(os.path.dirname(__file__), 'resource/test_product.csv')])
    gdf = cleanSchema(gdf)
    assert df.schema == gdf.schema