from pyspark.sql.functions import lit, current_timestamp, col
from datetime import datetime
import utils.paths as paths
import requests


dbutils.widgets.text("process_date", "")
process_date = dbutils.widgets.get("process_date")

if not process_date:
    process_date = datetime.now().strftime("%Y-%m-%d")

process_date_str = str(process_date)

input_path = paths.CSV_FILE_PUBLIC_PATH_LEFT + process_date_str + paths.CSV_FILE_PUBLIC_PATH_RIGHT

dbfs_path = "/dbfs/tmp/gastos-deputados_cota_parlamentar.csv"

# Download the file
response = requests.get(input_path)
with open(local_path, "wb") as f:
    f.write(response.content)

# Read with Spark
df = spark.read.csv(local_path, header=True)

df = df.withColumn("process_date", lit(process_date)).withColumn(
    "ingestion_ts", current_timestamp()
)

(
    df.write.mode("overwrite")
    .partitionBy("process_date")
    .format("delta")
    .saveAsTable("bronze.dataset_x")
)
