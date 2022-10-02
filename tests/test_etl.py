from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import Row, SparkSession
from pyspark.sql import types as T
from src import etl


class TestETL:
    def test(
        self,
        spark: SparkSession,
        input_schema: T.StructType,
        output_schema: T.StructType,
    ):
        # given
        given, then = get_given_then_data()
        src = spark.createDataFrame(data=given, schema=input_schema)

        # when
        result = etl.transform(src)

        # then
        dest = spark.createDataFrame(data=then, schema=output_schema)
        assert_df_equality(result, dest, ignore_nullable=True)


def get_given_then_data() -> tuple[list[Row], list[Row]]:
    given = [Row(title="Test Spark", price=10, author="Hyemi Noh", rate=5.0)]
    then = [
        Row(
            lower_title="test spark",
            price_won=10000,
            first_name="Hyemi",
            last_name="Noh",
            rate_commen="very good",
        )
    ]
    return given, then
