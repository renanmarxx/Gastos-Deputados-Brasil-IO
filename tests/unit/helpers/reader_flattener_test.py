from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def test_flatten(spark):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField(
                "info",
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("age", IntegerType(), True),
                    ]
                ),
                True,
            ),
        ]
    )
    data = [(1, {"name": "Alice", "age": 30}), (2, {"name": "Bob", "age": 25})]

    df = spark.createDataFrame(data, schema=schema)

    flattened_df = StructFlattener().flatten(df)

    expected_columns = ["id", "info_name", "info_age"]

    assert (
        flattened_df.columns == expected_columns
    ), "The Dataframe was not flattened correctly."
