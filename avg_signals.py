from pyspark.sql import SparkSession
from pyspark.sql.functions import hour
 
spark = SparkSession.builder \
    .appName("Average Signals Per Hour") \
    .getOrCreate()

delta_df = spark.read.format("delta").load("/home/xs451-anajai/kafka/deltatable/location")

delta_df = delta_df.withColumn("hour", hour("signal_ts"))

average_per_hour = delta_df.groupBy("hour").agg({"signals.LV_ActivePower": "avg",
                                                 "signals.Wind_Speed": "avg",
                                                 "signals.Theoretical_Power_Curve": "avg",
                                                 "signals.Wind_Direction": "avg"})
 
average_per_hour.show()
 
spark.stop()