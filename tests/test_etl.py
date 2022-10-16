from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import types as T
from src import etl


class TestETL:
    def test_success(
        self,
        spark: SparkSession,
        input_schema: T.StructType,
        output_schema: T.StructType,
    ):
        # given
        given_df, then_df = get_given_then_df(
            spark=spark, input_schema=input_schema, output_schema=output_schema
        )
        # when
        result = etl.transform(given_df)

        # then
        assert_df_equality(result, then_df, ignore_nullable=True)


def get_given_then_df(
    spark: SparkSession, input_schema: T.StructType, output_schema: T.StructType
) -> tuple[DataFrame, DataFrame]:
    given = [Row(title="Test Spark", price=10, author="Hyemi Noh", rate=5.0)]
    given_df = spark.createDataFrame(data=given, schema=input_schema)

    then = [
        Row(
            lower_title="test spark",
            price_won=10000,
            first_name="Hyemi",
            last_name="Noh",
            rate_commen="very good",
        )
    ]
    then_df = spark.createDataFrame(data=then, schema=output_schema)
    return given_df, then_df
