from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("sparkByExamples.com").getOrCreate()
csv_path = "D:\Padhayi\Liberary management system\kafka\kafka\T1 (1).xlsx"

schema= StructType([
StructField("Date/Time", StringType(), True),
StructField("LV_ActivePower", DoubleType(), True),
StructField("Wind_Speed", DoubleType(), True),
StructField("Theoretical_Power_Curve", DoubleType(), True),
StructField("Wind_Direction", DoubleType(), True)
])

df = spark.read\
    .schema(schema)\
    .csv(csv_path,header=True)

df.show()

df.selectExpr("to_json(struct(*)) AS value") \
.write \
.format("kafka") \
.option("kafka.bootstrap.servers", "localhost:9092") \
.option("topic", "quickstart-events") \
.save()
