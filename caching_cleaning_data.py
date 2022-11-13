import time
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession \
    .builder \
    .getOrCreate()

start_time = time.time()

# Define a new schema using the StructType method
departures_df = spark.read.format('csv').options(Header=True).load('AA_DFW_2018.csv.gz')

# Add caching to the unique rows in departures_df
departures_df = departures_df.cache().distinct()

# Count the unique rows in departures_df, noting how long the operation takes
# Count the rows again, noting the variance in time of a cached DataFrame
start_time = time.time()
print("Counting %d rows took %f seconds" % (departures_df.count(), time.time() - start_time))
print("Counting %d rows again took %f seconds" % (departures_df.count(), time.time() - start_time))

# Counting 139358 rows took 2.917033 seconds
# Counting 139358 rows again took 0.614624 seconds
