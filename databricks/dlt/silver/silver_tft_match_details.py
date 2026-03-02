df_bronze = spark.read.table("bronze.tft_match_details")
df_bronze.display()