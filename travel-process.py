import pyspark 
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("TEST").getOrCreate()

df_bids = spark.table("default.bids_txt")
df_exchange_rate = spark.table("default.exchange_rate_txt")
df_motels = spark.table("default.motels_txt")

#1.Erroneous records

# df_error = df_bids.select(col("BidDate"), col("HU")).where(col("HU").like("ERROR%"))
# df_error_hour = df_error.withColumn('BidDate',  hour(to_timestamp(col("BidDate"), "HH-dd-MM-yyyy")))
# df_error_count = df_error_hour.groupBy(col("BidDate").alias("Hour"), col("HU").alias("Type of Error")) \
#                               .agg(count("HU").alias("Corrupted records"))
# df_error_count.show(500)

#############################################################################################################
#3. Dealing with bids
df_counry = df_bids_result.select(col("MotelID"), \
                                   col("BidDate"), \
                                   col("US"), \
                                   col("CA"), \
                                   col("MX")).where(~ col("HU").like("ERROR%"))
df_map = df_counry.withColumn('map', \
                       create_map(lit("US"), df_counry.US,
                                  lit("CA"), df_counry.CA,
                                  lit("MX"), df_counry.MX
                                 )
                      )
df_dealing = df_map.select(col("MotelID"), col("BidDate"), explode(df_map.map) \
                       .alias("Country","Money")) \
                       .orderBy(col("BidDate"))

df_joined = df_dealing.alias("c").join(df_exchange_rate.alias("e"), col("c.BidDate") == col("e.ValidFrom")) \
                      .withColumn("Money", round((col("Money") * col("ExchangeRate")),2)) \
                      .withColumn("BidDate", to_timestamp(col("c.BidDate"), "HH-dd-MM-yyyy")) \
                      .select(col("c.MotelID"), col("BidDate"), col("Country"), col("Money")) \
                      .filter(col("Money").isNotNull()).cache()

# 5. Load motels 
# Join to the motels data
df_motel_join = df_joined.alias("j") \
                         .join(df_motels.alias("m"), (col("j.MotelID") == col("m.MotelID"))) \
                         .select(col("j.MotelID"), col("m.MotelName"), col("j.BidDate"), col("j.Country"), col("j.Money")).orderBy(col("j.BidDate"))


#Get max prices for a given motelId/bidDate
w = Window.partitionBy([col("MotelID"), col("BidDate")]).orderBy(col("Money").desc())
df_result = df_motel_join.withColumn("maxPrice", max(col("Money")).over(w)) \
                         .where(col("Money") == col("maxPrice")) \
                         .drop("maxPrice")

df_result.write.mode("overwrite").saveAsTable("default.result")
df_result.show()

