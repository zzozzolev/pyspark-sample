import os

from pyspark.sql import SparkSession

import etl

ROOT_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


def main():
    read_path = os.path.join(ROOT_PATH, "data/input/books.json")
    write_path = os.path.join(ROOT_PATH, "data/output/books")

    spark = SparkSession.builder.master("local[*]").getOrCreate()

    df = etl.extract(spark=spark, json_path=read_path)
    df = etl.transform(src=df)
    etl.load(src=df, path=write_path)


if __name__ == "__main__":
    main()
