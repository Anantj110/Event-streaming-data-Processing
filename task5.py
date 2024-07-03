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

json_df = [
    {"sig_name": "LV_ActivePower", "sig_mapping_name": "LV_ActivePower_average"},
    {"sig_name": "Wind_Speed", "sig_mapping_name": "Wind_Speed_average"},
    {"sig_name": "Theoretical_Power_Curve", "sig_mapping_name": "Theoretical_Power_Curve_average"},
    {"sig_name": "Wind_Direction", "sig_mapping_name": "Wind Direction_average"}
]
 
new_df = spark.createDataFrame([Row(**x) for x in json_df])
new_df.show()