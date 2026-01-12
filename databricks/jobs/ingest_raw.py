from pyspark.sql.functions import lit, current_timestamp, col
from datetime import datetime
import utils.paths as paths

dbutils.widgets.text("process_date", "")
process_date = dbutils.widgets.get("process_date")

if not process_date:
    process_date = datetime.now().strftime("%Y-%m-%d")

process_date_str = str(process_date)

#input_path = f"s3a://meu-bucket/dataset_x/raw/date={process_date}/"
#input_path = f"https://renan-marx-data-engineering-projects.s3.us-east-2.amazonaws.com/gastos-deputados-brasil-io/landing-bucket-gastos-deputados-brasil-io/dt%3D{process_date_str}/gastos-deputados_cota_parlamentar.csv.gz"
input_path = paths.CSV_FILE_PUBLIC_PATH_LEFT + process_date_str + paths.CSV_FILE_PUBLIC_PATH_RIGHT

df = spark.read.csv(input_path, header=True)    

#df = spark.read.option("header", "true").csv(input_path)

df = df.withColumn("process_date", lit(process_date)).withColumn(
    "ingestion_ts", current_timestamp()
)

(
    df.write.mode("overwrite")
    .partitionBy("process_date")
    .format("delta")
    .saveAsTable("bronze.dataset_x")
)
