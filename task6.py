import pyspark
from delta import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Distinct Datapoints Per Day") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.format("delta") \
  .load("/home/xs451-anajai/kafka/deltatable/location")

df = df.withColumn("generation_indicator",when(col("signals.LV_ActivePower") < 200, "Low")\
    .when((col("signals.LV_ActivePower") >= 200) & (col("signals.LV_ActivePower") < 600), "Medium")\
    .when((col("signals.LV_ActivePower") >= 600) & (col("signals.LV_ActivePower") < 1000), "High")\
    .otherwise("Exceptional"))

json_df = [
    {"sig_name": "LV_ActivePower", "sig_mapping_name": "LV_ActivePower_average"},
    {"sig_name": "Wind_Speed", "sig_mapping_name": "Wind_Speed_average"},
    {"sig_name": "Theoretical_Power_Curve", "sig_mapping_name": "Theoretical_Power_Curve_average"},
    {"sig_name": "Wind_Direction", "sig_mapping_name": "Wind Direction_average"}
]
 
new_df = spark.createDataFrame([Row(**x) for x in json_df])
new_df.show()

broadcast_df=df.join(broadcast(new_df),df["generation_indicator"] == new_df["sig_mapping_name"],"left_outer")
broadcast_df.show()