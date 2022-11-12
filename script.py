from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml import Pipeline, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
import pyspark.ml.evaluation as evals
import pyspark.ml.tuning as tune



# Average duration of Delta flights
flights.filter(flights.carrier == "DL").filter(flights.origin == "SEA").groupBy().avg("air_time").show()

# Total hours in the air
flights.withColumn("duration_hrs", flights.air_time / 60).groupBy().sum("duration_hrs").show()
#    +------------------+
#    |     avg(air_time)|
#    +------------------+
#    |188.20689655172413|
#    +------------------+

#    +------------------+
#    | sum(duration_hrs)|
#    +------------------+
#    |25289.600000000126|
#    +------------------+
# Group by tailnum
by_plane = flights.groupBy("tailnum")

# Number of flights each plane made
by_plane.count().show()

# Group by origin
by_origin = flights.groupBy("origin")

# Average duration of flights from PDX and SEA
by_origin.avg("air_time").show()
#    +-------+-----+
#    |tailnum|count|
#    +-------+-----+
#    | N442AS|   38|
#    | N102UW|    2|
#    | N36472|    4|
#    | N38451|    4|
#    | N73283|    4|
#    | N513UA|    2|
#    | N954WN|    5|
#    | N388DA|    3|
#    | N567AA|    1|
#    | N516UA|    2|
#    | N927DN|    1|
#    | N8322X|    1|
#    | N466SW|    1|
#    |  N6700|    1|
#    | N607AS|   45|
#    | N622SW|    4|
#    | N584AS|   31|
#    | N914WN|    4|
#    | N654AW|    2|
#    | N336NW|    1|
#    +-------+-----+
#    +------+------------------+
#    |origin|     avg(air_time)|
#    +------+------------------+
#    |   SEA| 160.4361496051259|
#    |   PDX|137.11543248288737|
#    +------+------------------+

# Group by month and dest
by_month_dest = flights.groupBy("dest", "month")

# Average departure delay by month and destination
by_month_dest.avg("dep_delay").show()

# Standard deviation of departure delay
by_month_dest.agg(F.stddev("dep_delay")).show()

# Examine the data
print(airports.show())

# Rename the faa column
airports = airports.withColumnRenamed("faa", "dest")

# Join the DataFrames
flights_with_airports = flights.join(airports, on='dest', how='leftouter')

# Examine the new DataFrame
print(flights_with_airports.show())
# +----+----+-----+---+--------+---------+--------+---------+-------+-------+------+------+--------+--------+----+------+--------------------+---------+-----------+----+---+---+
# |dest|year|month|day|dep_time|dep_delay|arr_time|arr_delay|carrier|tailnum|flight|origin|air_time|distance|hour|minute|                name|      lat|        lon| alt| tz|dst|
# +----+----+-----+---+--------+---------+--------+---------+-------+-------+------+------+--------+--------+----+------+--------------------+---------+-----------+----+---+---+
# | LAX|2014|   12|  8|     658|       -7|     935|       -5|     VX| N846VA|  1780|   SEA|     132|     954|   6|    58|    Los Angeles Intl|33.942536|-118.408075| 126| -8|  A|
# | HNL|2014|    1| 22|    1040|        5|    1505|        5|     AS| N559AS|   851|   SEA|     360|    2677|  10|    40|       Honolulu Intl|21.318681|-157.922428|  13|-10|  N|
# | SFO|2014|    3|  9|    1443|       -2|    1652|        2|     VX| N847VA|   755|   SEA|     111|     679|  14|    43|  San Francisco Intl|37.618972|-122.374889|  13| -8|  A|
# | SJC|2014|    4|  9|    1705|       45|    1839|       34|     WN| N360SW|   344|   PDX|      83|     569|  17|     5|Norman Y Mineta S...|  37.3626|-121.929022|  62| -8|  A|
# | BUR|2014|    3|  9|     754|       -1|    1015|        1|     AS| N612AS|   522|   SEA|     127|     937|   7|    54|            Bob Hope|34.200667|-118.358667| 778| -8|  A|
# | DEN|2014|    1| 15|    1037|        7|    1352|        2|     WN| N646SW|    48|   PDX|     121|     991|  10|    37|         Denver Intl|39.861656|-104.673178|5431| -7|  A|
# | OAK|2014|    7|  2|     847|       42|    1041|       51|     WN| N422WN|  1520|   PDX|      90|     543|   8|    47|Metropolitan Oakl...|37.721278|-122.220722|   9| -8|  A|
# | SFO|2014|    5| 12|    1655|       -5|    1842|      -18|     VX| N361VA|   755|   SEA|      98|     679|  16|    55|  San Francisco Intl|37.618972|-122.374889|  13| -8|  A|
# | SAN|2014|    4| 19|    1236|       -4|    1508|       -7|     AS| N309AS|   490|   SEA|     135|    1050|  12|    36|      San Diego Intl|32.733556|-117.189667|  17| -8|  A|
# | ORD|2014|   11| 19|    1812|       -3|    2352|       -4|     AS| N564AS|    26|   SEA|     198|    1721|  18|    12|  Chicago Ohare Intl|41.978603| -87.904842| 668| -6|  A|
# | LAX|2014|   11|  8|    1653|       -2|    1924|       -1|     AS| N323AS|   448|   SEA|     130|     954|  16|    53|    Los Angeles Intl|33.942536|-118.408075| 126| -8|  A|
# | PHX|2014|    8|  3|    1120|        0|    1415|        2|     AS| N305AS|   656|   SEA|     154|    1107|  11|    20|Phoenix Sky Harbo...|33.434278|-112.011583|1135| -7|  N|
# | LAS|2014|   10| 30|     811|       21|    1038|       29|     AS| N433AS|   608|   SEA|     127|     867|   8|    11|      Mc Carran Intl|36.080056| -115.15225|2141| -8|  A|
# | ANC|2014|   11| 12|    2346|       -4|     217|      -28|     AS| N765AS|   121|   SEA|     183|    1448|  23|    46|Ted Stevens Ancho...|61.174361|-149.996361| 152| -9|  A|
# | SFO|2014|   10| 31|    1314|       89|    1544|      111|     AS| N713AS|   306|   SEA|     129|     679|  13|    14|  San Francisco Intl|37.618972|-122.374889|  13| -8|  A|
# | SFO|2014|    1| 29|    2009|        3|    2159|        9|     UA| N27205|  1458|   PDX|      90|     550|  20|     9|  San Francisco Intl|37.618972|-122.374889|  13| -8|  A|
# | SMF|2014|   12| 17|    2015|       50|    2150|       41|     AS| N626AS|   368|   SEA|      76|     605|  20|    15|     Sacramento Intl|38.695417|-121.590778|  27| -8|  A|
# | MDW|2014|    8| 11|    1017|       -3|    1613|       -7|     WN| N8634A|   827|   SEA|     216|    1733|  10|    17| Chicago Midway Intl|41.785972| -87.752417| 620| -6|  A|
# | BOS|2014|    1| 13|    2156|       -9|     607|      -15|     AS| N597AS|    24|   SEA|     290|    2496|  21|    56|General Edward La...|42.364347| -71.005181|  19| -5|  A|
# | BUR|2014|    6|  5|    1733|      -12|    1945|      -10|     OO| N215AG|  3488|   PDX|     111|     817|  17|    33|            Bob Hope|34.200667|-118.358667| 778| -8|  A|
# +----+----+-----+---+--------+---------+--------+---------+-------+-------+------+------+--------+--------+----+------+--------------------+---------+-----------+----+---+---+
# Rename year column
planes = planes.withColumnRenamed('year', 'plane_year')
# Join the DataFrames
model_data = flights.join(planes, on='tailnum', how="leftouter")

# Cast the columns to integers
model_data = model_data.withColumn("arr_delay", model_data.arr_delay.cast('integer'))
model_data = model_data.withColumn("air_time", model_data.air_time.cast('integer'))
model_data = model_data.withColumn("month", model_data.month.cast('integer'))
model_data = model_data.withColumn("plane_year", model_data.plane_year.cast('integer'))

# Create is_late
model_data = model_data.withColumn("is_late", model_data.arr_delay > 0)
# Convert to an integer
model_data = model_data.withColumn("label", model_data.is_late.cast('integer'))
# Remove missing values
model_data = model_data.filter("arr_delay is not NULL and dep_delay is not NULL and air_time is not NULL and plane_year is not NULL")

# Create a StringIndexer
carr_indexer = StringIndexer(
    inputCol="carrier",
    outputCol="carrier_index"
)

# Create a OneHotEncoder
carr_encoder = OneHotEncoder(
    inputCol='carrier_index',
    outputCol='carrier_fact'
)

# Create a StringIndexer
dest_indexer = StringIndexer(inputCol='dest', outputCol='dest_index')

# Create a OneHotEncoder
dest_encoder = OneHotEncoder(inputCol='dest_index', outputCol='dest_fact')

# Make a VectorAssembler
vec_assembler = VectorAssembler(inputCols=["month", "air_time", "carrier_fact", "dest_fact", "plane_age"], outputCol='features')

# Make the pipeline
flights_pipe = Pipeline(stages=[
    dest_indexer, dest_encoder, carr_indexer, carr_encoder, vec_assembler
])

# Fit and transform the data
piped_data = flights_pipe.fit(model_data).transform(model_data)

# Split the data into training and test sets
# training with 60% of the data, and test with 40% of the data by passing the list [.6, .4] to the .randomSplit() method.
training, test = piped_data.randomSplit([.6, .4])

# Create a LogisticRegression Estimator
lr = LogisticRegression()

# Create a BinaryClassificationEvaluator
evaluator = evals.BinaryClassificationEvaluator(metricName='areaUnderROC')

# Create the parameter grid
grid = tune.ParamGridBuilder()

# Add the hyperparameter
grid = grid.addGrid(lr.regParam, np.arange(0, .1, .01))
grid = grid.addGrid(lr.elasticNetParam, [0, 1])

# Build the grid
grid = grid.build()

cv = tune.CrossValidator(
    estimator=lr,
    estimatorParamMaps=grid,
    evaluator=evaluator
)

# Call lr.fit()
best_lr = lr.fit(training)

# Print best_lr
print(best_lr)
