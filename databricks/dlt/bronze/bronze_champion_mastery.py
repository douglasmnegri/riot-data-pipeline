from pyspark.sql.functions import current_timestamp

storage_account = "striotdatapipeline001"

RAW_PATH = f"abfss://riot-data@{storage_account}.dfs.core.windows.net/data/riot/lol/champion_mastery"

df = (
    spark.read
        .option("multiLine", "true")
        .json(RAW_PATH)
)

display(df)