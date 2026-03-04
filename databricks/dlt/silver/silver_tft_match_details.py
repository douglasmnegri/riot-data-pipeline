from pyspark.sql.functions import explode

storage_account = "striotdatapipeline001"

SILVER_BASE_PATH = f"abfss://riot-data@{storage_account}.dfs.core.windows.net/delta/silver"

SILVER_MATCHES_PATH = f"{SILVER_BASE_PATH}/tft_matches"
SILVER_PARTICIPANTS_PATH = f"{SILVER_BASE_PATH}/tft_participants"
SILVER_UNITS_PATH = f"{SILVER_BASE_PATH}/tft_units"
SILVER_TRAITS_PATH = f"{SILVER_BASE_PATH}/tft_traits"
SILVER_UNIT_ITEMS_PATH = f"{SILVER_BASE_PATH}/tft_unit_items"

df_bronze = spark.read.table("bronze.tft_match_details")


# MATCHES (1 row per game)

df_matches = (
    df_bronze
        .select(
            "info.gameId",
            "info.gameCreation",
            "info.game_datetime",
            "info.queueId",
            "info.endOfGameResult",
            "info.game_length",
            "ingestion_timestamp",
            "source_file"
        )
        .dropDuplicates(["gameId"])
)


# Base Participants Explosion (once)

df_participants_base = (
    df_bronze
        .select(
            "info.gameId",
            explode("info.participants").alias("p"),
            "ingestion_timestamp",
            "source_file"
        )
)


# PARTICIPANTS (1 row per player)

df_participants = (
    df_participants_base
        .select(
            "gameId",
            "p.puuid",
            "p.placement",
            "p.level",
            "p.gold_left",
            "p.last_round",
            "p.players_eliminated",
            "p.riotIdGameName",
            "p.time_eliminated",
            "p.total_damage_to_players",
            "ingestion_timestamp",
            "source_file"
        ).dropDuplicates(["gameId", "puuid"])
)

# UNITS (1 row per unit)

df_units = (
    df_participants_base
        .select(
            "gameId",
            "p.puuid",
            explode("p.units").alias("unit"),
            "ingestion_timestamp",
            "source_file"
        )
        .select(
            "gameId",
            "puuid",
            "unit.character_id",
            "unit.tier",
            "unit.rarity",
            "ingestion_timestamp",
            "source_file"
        ).dropDuplicates(["gameId", "puuid", "character_id"])
)


# UNIT ITEMS (1 row per item per unit)

df_unit_items = (
    df_participants_base
        .select(
            "gameId",
            "p.puuid",
            explode("p.units").alias("unit"),
            "ingestion_timestamp",
            "source_file"
        )
        .select(
            "gameId",
            "puuid",
            "unit.character_id",
            explode("unit.itemNames").alias("item_name"),
            "ingestion_timestamp",
            "source_file"
        ).dropDuplicates(["gameId", "puuid", "character_id", "item_name"])
)


# TRAITS (1 row per trait)

df_traits = (
    df_participants_base
        .select(
            "gameId",
            "p.puuid",
            explode("p.traits").alias("trait"),
            "ingestion_timestamp",
            "source_file"
        )
        .select(
            "gameId",
            "puuid",
            "trait.name",
            "trait.num_units",
            "trait.style",
            "trait.tier_current",
            "trait.tier_total",
            "ingestion_timestamp",
            "source_file"
        ).dropDuplicates(["gameId", "puuid", "name"])
)


# CREATE SCHEMA

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS silver
    MANAGED LOCATION '{SILVER_BASE_PATH}'
""")


# WRITE DELTA FILES

df_matches.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(SILVER_MATCHES_PATH)

df_participants.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(SILVER_PARTICIPANTS_PATH)

df_units.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(SILVER_UNITS_PATH)

df_traits.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(SILVER_TRAITS_PATH)

df_unit_items.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(SILVER_UNIT_ITEMS_PATH)


# REGISTER TABLES

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver.tft_matches
    USING DELTA
    LOCATION '{SILVER_MATCHES_PATH}'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver.tft_participants
    USING DELTA
    LOCATION '{SILVER_PARTICIPANTS_PATH}'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver.tft_units
    USING DELTA
    LOCATION '{SILVER_UNITS_PATH}'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver.tft_traits
    USING DELTA
    LOCATION '{SILVER_TRAITS_PATH}'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver.tft_unit_items
    USING DELTA
    LOCATION '{SILVER_UNIT_ITEMS_PATH}'
""")