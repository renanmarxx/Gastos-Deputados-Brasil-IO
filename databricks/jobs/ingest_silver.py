import utils.functions as f

dbutils.widgets.text("process_date", "")
process_date = dbutils.widgets.get("process_date")

df = spark.table("bronze.dataset_x").filter(col("process_date") == process_date)

df_clean = df.dropDuplicates(["id"]).withColumn("valor", col("valor").cast("double"))

(
    df_clean.write.mode("overwrite")
    .partitionBy("process_date")
    .format("delta")
    .saveAsTable("silver.dataset_x")
)
