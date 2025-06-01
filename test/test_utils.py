import os.path

import pytest
from pyspark.sql import SparkSession
from src.utils import getUnprocessedFiles
from unittest.mock import MagicMock, patch
from datetime import datetime
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


def test_getUnprocessedFiles(spark):
    mock_max_ts = int(datetime(2024, 1, 1, 12, 0, 0).timestamp()*1000)

    # Mock Spark SQL result
    mock_row = MagicMock()
    mock_row.__getitem__.return_value = mock_max_ts
    mock_collect = [mock_row]

    # Mock dbutils.fs.ls result
    mock_file_1 = MagicMock()
    mock_file_1.path = "/mnt/data/incoming/products/file1.json"
    mock_file_1.modificationTime = int(datetime(2024, 1, 1, 13, 0, 0).timestamp() * 1000)  # newer

    mock_file_2 = MagicMock()
    mock_file_2.path = "/mnt/data/incoming/products/file2.json"
    mock_file_2.modificationTime = int(datetime(2023, 12, 31, 13, 0, 0).timestamp() * 1000)  # older

    with patch.object(spark, 'sql') as mock_sql, \
            patch('builtins.dbutils',create=True) as mock_dbutils:
        # Mocking Spark SQL query
        mock_sql.return_value.collect.return_value = mock_collect

        # Mocking dbutils.fs.ls
        mock_dbutils.fs.ls.return_value = [mock_file_1, mock_file_2]

        result = getUnprocessedFiles(spark, "/mnt/data/incoming/products/", "product")
        assert len(result) == 1
        assert result[0]["file_path"] == "/mnt/data/incoming/products/file1.json"
        assert result[0]["object"] == "product"
        assert result[0]["modification_time"] == mock_file_1.modificationTime

def test_getUnprocessedFiles_None(spark):
    mock_max_ts = None

    # Mock Spark SQL result
    mock_row = MagicMock()
    mock_row.__getitem__.return_value = mock_max_ts
    mock_collect = [mock_row]

    # Mock dbutils.fs.ls result
    mock_file_1 = MagicMock()
    mock_file_1.path = "/mnt/data/incoming/products/file1.json"
    mock_file_1.modificationTime = int(datetime(2024, 1, 1, 13, 0, 0).timestamp() * 1000)  # newer

    mock_file_2 = MagicMock()
    mock_file_2.path = "/mnt/data/incoming/products/file2.json"
    mock_file_2.modificationTime = int(datetime(2023, 12, 31, 13, 0, 0).timestamp() * 1000)  # older

    with patch.object(spark, 'sql') as mock_sql, \
            patch('builtins.dbutils',create=True) as mock_dbutils:
        # Mocking Spark SQL query
        mock_sql.return_value.collect.return_value = mock_collect

        # Mocking dbutils.fs.ls
        mock_dbutils.fs.ls.return_value = [mock_file_1, mock_file_2]

        result = getUnprocessedFiles(spark, "/mnt/data/incoming/products/", "product")
        assert len(result) == 2
        assert result[0]["file_path"] == "/mnt/data/incoming/products/file1.json"
        assert result[0]["object"] == "product"
        assert result[0]["modification_time"] == mock_file_1.modificationTime
        assert result[1]["file_path"] == "/mnt/data/incoming/products/file2.json"
        assert result[1]["object"] == "product"
        assert result[1]["modification_time"] == mock_file_2.modificationTime