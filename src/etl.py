from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def extract(spark: SparkSession, json_path: str) -> DataFrame:
    return spark.read.json(json_path, multiLine=True)


def transform(src: DataFrame) -> DataFrame:
    return src.select(
        F.lower("title").alias("lower_title"),
        (F.col("price") * 1000).alias("price_won"),
        F.split("author", " ")[0].alias("first_name"),
        F.split("author", " ")[1].alias("last_name"),
        F.when(condition=F.col("rate") >= 4.0, value="very good")
        .otherwise("good")
        .alias("rate_comment"),
    )


def load(src: DataFrame, path: str):
    src.write.format("json").mode("overwrite").save(path)
