import utils.functions as f

dbutils.widgets.text("process_date", "")
process_date = dbutils.widgets.get("process_date")

input_path = f"s3a://meu-bucket/dataset_x/raw/date={process_date}/"

df = (
    spark.read
    .option("header", "true")
    .csv(input_path)
)

df = (
    df
    .withColumn("process_date", lit(process_date))
    .withColumn("ingestion_ts", current_timestamp())
)

(
    df.write
    .mode("overwrite")
    .partitionBy("process_date")
    .format("delta")
    .saveAsTable("bronze.dataset_x")
)