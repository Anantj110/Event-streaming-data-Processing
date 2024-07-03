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

df.show()

df.createOrReplaceTempView("df2")
spark.sql("select signal_date,count(distinct(signal_ts)) from df2 group by signal_date").show()
