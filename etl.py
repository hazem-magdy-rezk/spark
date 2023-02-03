import configparser
from datetime import datetime
import os
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import IntegerType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import TimestampType as TimeStamp
from pyspark.sql.types import StructType, StructField, DoubleType, StringType , IntegerType

config = configparser.ConfigParser()

config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']

os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
def create_spark_session():
    # creation of spark session
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
   a function to  load the song data from s3 bucket and create        the tables for songs and artists then laoad them back to s3 as      parquet files
   Args:
   spark(pyspark.sql.SparkSession): SparkSession object
   input_data(string): Path to input data directory. 
e.g: 's3a://udacity-dend/song_data/A/A/B/TRAABJL12903CDCF1A.json'
    output_data(string): Path to output data directory.
e.g. 's3a://udacity-projects-bucket/songs'
   Returns:
        None
    """
    # setting schema for song_data
    song_schema = StructType([        
    StructField("song_id", StringType()),
    StructField("title", StringType()),
    StructField("artist_name", StringType()),        
    StructField("artist_id", StringType()),
    StructField("artist_latitude", DoubleType()),
    StructField("artist_longitude", DoubleType()),
    StructField("artist_location", DoubleType()),
    StructField("num_songs", DoubleType()),
    StructField("duration", DoubleType()),
    StructField("year", IntegerType())])

    # get filepath to song data file
    #song_data = input_data+'song_data/*/*/*/*.json'
    song_data =  input_data+'song_data/A/A/A/*.json'
    
    # read song data file
    df = spark.read.json(song_data, schema=song_schema)
   
    # extract columns to create songs table
    songs_table=   df.select(["song_id","title","artist_id","year","duration"]).dropDuplicates()
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').parquet(output_data+'songs.parquet')

    # extract columns to create artists table
    artists_table =     df.select(["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]).dropDuplicates()
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists.parquet')

def process_log_data(spark, input_data, output_data):
    """
   a function to load  log data from s3bucket  and store it at s3 bucket  as parquet files.
   Args:
     spark(pyspark.sql.SparkSession): SparkSession object
     input_data(string): Path to input data directory. 
       e.g. 'data/log/log/2018/11/2018-11-12-events.json'
     output_data(string): Path to output data directory. 
       e.g. 's3a://udacity-projects-bucket/users'
    Returns:
        None
    """
    
    #setting schema for log_data
    log_schema=StructType([
        StructField("artist",StringType()),
        StructField("auth",StringType()),
        StructField("firstName",StringType()),
        StructField("gender",StringType()),
        StructField("itemInSession",IntegerType()),
        StructField("lastName",StringType()),
        StructField("length",DoubleType()),
        StructField("level",StringType()),
        StructField("location",StringType()),
        StructField("method",StringType()),
        StructField("page",StringType()),
        StructField("registration",DoubleType()),
        StructField("sessionId",IntegerType()),
        StructField("song",StringType()),
        StructField("status",IntegerType()),
        StructField("ts",IntegerType()),
        StructField("userAgent",StringType()),
        StructField("userId",IntegerType())
    ])
        
    # get filepath to log data file
    #log_data =input_data+'log_data/*/*/*.json'
    log_data =input_data+'log_data/2018/11/2018-11-12-events.json'

    # read log data file
    df = spark.read.json(log_data,schema=log_schema)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table=    df.select(["userId","firstName","lastName","gender","level"]).dropDuplicates()
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users.parquet')  
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: to_date(x), TimestampType())
    df = df.withColumn("datetime", get_datetime(df.ts))    
    # extract columns to create time table 
    time_table= df.selectExpr(
      "timestamp as start_time",
      "hour(timestamp) as hour",
      "dayofmonth(timestamp) as day",
      "weekofyear(timestamp) as week",
      "month(timestamp) as month",
      "year(timestamp) as year",
      "dayofweek(timestamp) as weekday"
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data+"time.parquet")

   # read in song data to use for songplays table
   #song_data='song_data/*/*/*/*.json' 
    song_data='song_data/A/A/A/*.json'
    song_df = spark.read.json(input_data+song_data)
    df = df.withColumn('songplay_id', F.monotonically_increasing_id())
    song_df.createOrReplaceTempView('song_table')
    df.createOrReplaceTempView('log_table')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
SELECT DISTINCT
      l.timestamp as start_time,
      year(l.timestamp) as year,
      month(l.timestamp) as month,
      l.userId AS user_id,
      l.level as level,
      s.song_id as song_id,
      s.artist_id as artist_id,
      l.sessionId as session_id,
      l.location as location,
      l.userAgent as user_agent
FROM  log_table l
JOIN  song_table s 
ON    l.song = s.title 
AND   l.artist = s.artist_name 
AND   l.length = s.duration
WHERE l.page='NextSong'
    """)
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year",     "month").parquet(output_data+"songplays.parquet")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-projects-bucket/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
