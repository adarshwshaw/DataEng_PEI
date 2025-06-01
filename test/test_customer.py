import os.path

import pytest
from pyspark.sql import SparkSession
from src.customer import clean_customer_data
from pyspark.sql.types import *
from pyspark.sql.functions import *

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("PySpark Pytest") \
        .master("local[*]") \
        .getOrCreate()
    print("SparkSession started.")
    yield spark
    spark.stop()
    print("SparkSession stopped.")

@pytest.fixture(scope="module")
def src_df(spark):
    file = os.path.join(os.path.dirname(__file__), 'resource/test_customer_src.json')
    src_df = spark.read.option("header", True).option("mode", "PERMISSIVE") \
        .option("multiLine", True).json(file)
    yield src_df

@pytest.fixture(scope="module")
def exp_df(spark):
    file = os.path.join(os.path.dirname(__file__), 'resource/test_customer_exp.json')
    exp_df = spark.read.option("header", True).option("mode", "PERMISSIVE") \
        .option("multiLine", True).json(file)
    yield exp_df

def test_clean_customer_data(spark,src_df,exp_df):
    # src_df.show()
    df = clean_customer_data(src_df)
    res = df.collect()
    ex_res = exp_df.collect()
    assert res == ex_res
