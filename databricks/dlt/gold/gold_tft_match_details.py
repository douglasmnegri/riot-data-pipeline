storage_account = "striotdatapipeline001"

GOLD_BASE_PATH = f"abfss://riot-data@{storage_account}.dfs.core.windows.net/delta/gold"

GOLD_PLAYER_STATS_PATH = f"{GOLD_BASE_PATH}/tft_player_stats"
GOLD_TRAIT_STATS_PATH = f"{GOLD_BASE_PATH}/tft_trait_stats"
GOLD_UNIT_STATS_PATH = f"{GOLD_BASE_PATH}/tft_unit_stats"


df_gold_most_played_traits = spark.sql("""
SELECT
    name as trait_name,
    COUNT(name) AS number_of_trait_appearence
FROM silver.tft_traits
GROUP BY name
ORDER BY number_of_trait_appearence DESC
""")

df_gold_traits_placement = spark.sql("""
SELECT
    t.name AS trait_name,
    t.num_units,
    COUNT(*) AS appearances,
    AVG(p.placement) AS avg_placement
FROM silver.tft_traits t
JOIN silver.tft_participants p
    ON t.gameId = p.gameId
    AND t.puuid = p.puuid
GROUP BY t.name, t.num_units
ORDER BY t.name, t.num_units
""")

df_gold_units = spark.sql("""
SELECT
    character_id as unit,
    COUNT(*) AS appearances
FROM silver.tft_units
GROUP BY character_id
ORDER BY appearances DESC
""")