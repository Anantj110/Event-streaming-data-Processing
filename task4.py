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
 
df.show(10)
