import dlt
from pyspark.sql.functions import current_timestamp

spark.conf.set(
    "fs.azure.account.key.riotdata.blob.core.windows.net",
    dbutils.secrets.get("riot-scope", "riotdata-access-key")
)

RAW_PATH = "wasbs://riot-data-pipeline@riotdata.blob.core.windows.net/puuids/champion_mastery"

@dlt.table(
    name="matches_bronze",
    comment="Raw champion mastery JSON ingested from Riot API",
    table_properties={"quality": "bronze"}
)
def matches_bronze():
    return (
        spark.read
            .option("multiLine", "true")
            .json(RAW_PATH)
            .withColumn("_ingestion_ts", current_timestamp())
    )
