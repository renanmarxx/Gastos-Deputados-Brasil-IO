from databricks.sdk.runtime import *  # noqa: F403
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import udf as U
from pyspark.sql.session import SparkSession

udf = U
spark: SparkSession
sc = spark.sparkContext  # noqa: F821
sqlContext: SQLContext
sql = sqlContext.sql  # noqa: F821
table = sqlContext.table  # noqa: F821
getArgument = dbutils.widgets.getArgument  # noqa: F405


def displayHTML(html): ...
def display(input=None, *args, **kwargs): ...
