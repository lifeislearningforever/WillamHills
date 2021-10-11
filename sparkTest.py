from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *


appName = "PySpark Hive Example"
master = "local"

# Create Spark session with Hive supported.
spark = SparkSession.builder \
    .appName(appName) \
    .config("spark.sql.warehouse.dir","/user/hive/warehouse") \
    .master(master) \
    .enableHiveSupport() \
    .getOrCreate()
# spark.sql('create database movies')
spark.sql('use movies')
# spark.sql('show tables').show()
# spark.sql('create table movies \
#          (movieId int,title string,genres string) \
#          row format delimited fields terminated by ","\
#          stored as textfile')
#
# spark.sql("create table ratings\
#            (userId int,movieId int,rating float,timestamp string)\
#            stored as ORC" )
#
# spark.sql("create table genres_by_count\
#            ( genres string,count int)\
#            stored as AVRO" )

# spark.sql("describe formatted ratings").show(truncate = False)

# spark.sql("load data local inpath '/Users/prakashhosalli/Downloads/ml-latest-small/movies.csv' overwrite into table movies")

schema = StructType([
             StructField('userId', IntegerType()),
             StructField('movieId', IntegerType()),
             StructField('rating', DoubleType()),
             StructField('timestamp', StringType())
            ])

ratings_df = spark.read.csv("/user/prakash/ratings.csv", schema=schema, header=True,)

# ratings_df.("ratings")
ratings_df = spark.sql("select * from ratings")
ratings_df.show(5)

