import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from datetime import date
import unittest
from sparkutils import col_min, col_max

spark = (
    SparkSession.builder
    .master('local')
    .appName('tester')
    .getOrCreate()
)

test_df = spark.createDataFrame(
    [
        (1, date(2018, 1, 1), 'b'),
        (3, date(2016, 7, 1), 'c'),
        (2, date(2014, 1, 1), 'a')
    ],
    schema=['col_int', 'col_date', 'col_str']
)

class TestMinFunc(unittest.TestCase):

    def test_int(self):
        result = col_min(test_df, 'col_int')
        expected = 1
        self.assertEqual(result, expected)

    def test_date(self):
        result = col_min(test_df, 'col_date')
        expected = date(2014, 1, 1)
        self.assertEqual(result, expected)

    def test_str(self):
        result = col_min(test_df, 'col_str')
        expected = 'a'
        self.assertEqual(result, expected)

class TestMaxFunc(unittest.TestCase):

    def test_int(self):
        result = col_max(test_df, 'col_int')
        expected = 3
        self.assertEqual(result, expected)

    def test_date(self):
        result = col_max(test_df, 'col_date')
        expected = date(2018, 1, 1)
        self.assertEqual(result, expected)

    def test_str(self):
        result = col_max(test_df, 'col_str')
        expected = 'c'
        self.assertEqual(result, expected)

