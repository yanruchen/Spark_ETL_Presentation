import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read('dl.cfg')
print(config.items('AWS'))

aws_access_key_id = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key_id
aws_secret = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret


def create_spark_session():
    """
    this function creates a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    this function process song data, and use its data to populate songs_table and artists_table
    """
    # get filepath to song data file
    # tested using subset song_data/A/A/A/*.json
    song_data = input_data + 'song_data/*/*/*/*.json' 
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'year', 'duration', 'artist_id').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs_table')


    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')\
                    .dropDuplicates()

    
    # write artists table to parquet files
    artists_table = artists_table.write.mode("overwrite").parquet(output_data + 'artists_table')



def process_log_data(spark, input_data, output_data):
    """
    this function process log data, and use its data to populate users_table and time_table
    this function also create fact table songplays_table by join log_data and song_data
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'Next Song')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates(['userId'])
    
    # write users table to parquet files
    users_table = users_table.write.mode("overwrite").parquet(output_data + 'users_table')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x / 1000.0)), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp((x / 1000.0)), DateType())
    df = df.withColumn("datetime", get_datetime(col("ts")))
    
    # created log view to write SQL Queries (this is needed for the following code)
    df.createOrReplaceTempView("log_data")
    
    # extract columns to create time table
    time_table = spark.sql("""
        SELECT DISTINCT timestamp AS start_time,
                    hour(timestamp) AS hour,
                    day(timestamp) AS day,
                    weekofyear(timestamp) AS week,
                    month(timestamp) AS month,
                    year(timestamp) AS year,
                    dayofweek(timestamp) AS weekday
        FROM log_data
        WHERE timestamp IS NOT NULL
    
    """).dropDuplicates(['start_time']) 
    
    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.partitionBy('year', 'month').parquet(output_data + 'time_table')
    

    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'
    song_df = spark.read.json(song_data)
    
    # created song view to write SQL Queries (this is needed for the following code)
    song_df.createOrReplaceTempView("song_data")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT monotonically_increasing_id() AS songplay_id,
               l.timestamp AS start_time,
               l.userId AS user_id,
               l.level,
               s.song_id,
               s.artist_id,
               l.sessionId AS session_id,
               s.artist_location AS location,
               l.userAgent AS user_agent
        FROM log_data l
        INNER JOIN song_data s
        ON l.song = s.title
        
    """).dropDuplicates(['start_time']) 
    

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.partitionBy('year', 'month').parquet(output_data + 'songplays_table')


def main():
    """
    this function is the main function, which create the spark session and input/output objects,
    and execute process_song_data and process_log_data function.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalakesawsbucket/" #created s3 bucket with name "datalakesawsbucket"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
