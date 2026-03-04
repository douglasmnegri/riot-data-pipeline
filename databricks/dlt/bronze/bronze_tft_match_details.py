from pyspark.sql.functions import current_timestamp, input_file_name

storage_account = "striotdatapipeline001"

RAW_PATH = f"abfss://riot-data@{storage_account}.dfs.core.windows.net/data/riot/tft/match_details"
BRONZE_PATH = f"abfss://riot-data@{storage_account}.dfs.core.windows.net/delta/bronze/tft_match_details"

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS bronze
MANAGED LOCATION 'abfss://riot-data@{storage_account}.dfs.core.windows.net/delta/bronze'
""")

df_raw = (
    spark.read
        .option("multiLine", "true")
        .json(RAW_PATH)
)

df_bronze = (
    df_raw
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
)


df_bronze.write \
    .format("delta") \
    .mode("append") \
    .save(BRONZE_PATH)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS bronze.tft_match_details
USING DELTA
LOCATION '{BRONZE_PATH}'
""")

display(spark.read.table("bronze.tft_match_details"))