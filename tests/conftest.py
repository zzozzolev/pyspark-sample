import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return SparkSession.builder.master("local[*]").getOrCreate()


@pytest.fixture(scope="session")
def input_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("title", T.StringType(), True),
            T.StructField("price", T.IntegerType(), True),
            T.StructField("author", T.StringType(), True),
            T.StructField("rate", T.FloatType(), True),
        ]
    )


@pytest.fixture(scope="session")
def output_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("lower_title", T.StringType(), True),
            T.StructField("price_won", T.IntegerType(), True),
            T.StructField("first_name", T.StringType(), True),
            T.StructField("last_name", T.StringType(), True),
            T.StructField("rate_comment", T.StringType(), True),
        ]
    )
