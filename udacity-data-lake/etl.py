#!/usr/bin/env python3

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofweek, dayofyear, hour, weekofyear, date_format
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import TimestampType
from pyspark.sql.types import DateType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['aws']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['aws']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Load the song data from the S3 location defined in input_data
    and extract the song and artist tables from it as dataframes. 
    These dataframes are then put to the S3 path passed in via output_data
    as spark parquet files
    """

    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*")

    # read song data file
    df = spark.read.format('json').load(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        'song_id', 
        'title', 
        'artist_id', 
        'year', 
        'duration').dropDuplicates(subset=['song_id'])

    # write songs table to parquet files partitioned by year and artist
    table_path = os.path.join(output_data, "songs")
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(table_path)

    # extract columns to create artists table
    artists_table = df.select(
        'artist_id', 
        col('artist_name').alias('name'),
        col('artist_location').alias('location'), 
        col('artist_latitude').alias('latitude'), 
        col('artist_longitude').alias('longitude')).dropDuplicates(subset=['artist_id'])

    # write artists table to parquet files
    table_path = os.path.join(output_data, "artists")
    artists_table.write.mode('overwrite').parquet(table_path)


def process_log_data(spark, input_data, output_data):
    """
    Load and process the event data to build the user, time and songplay
    tables. These are then put to the S3 location as parquet files.
    """

    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*")

    # read log data file
    df = spark.read.format('json').load(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.select(
        col('userId').alias('user_id'), 
        col('firstName').alias('first_name'), 
        col('lastName').alias('last_name'), 
        'gender', 
        'level').dropDuplicates(subset=['user_id'])

    # write users table to parquet files
    table_path = os.path.join(output_data, "users")
    users_table.write.mode('overwrite').parquet(table_path)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(
        float(ts)/1000.0), TimestampType())

    df = df.withColumn('timestamp', get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(
        float(ts)/1000.0), DateType())

    df = df.withColumn("datetime", get_datetime('ts'))

    # extract columns to create time table
    time_table = df.select(
        col('ts').alias('start_time'), 
        'timestamp', 
        'datetime',
        hour('timestamp').alias('hour'),
        dayofyear('timestamp').alias('day'),
        weekofyear('timestamp').alias('week'),
        month('timestamp').alias('month'),
        year('timestamp').alias('year'),
        dayofweek('timestamp').alias('weekday')).dropDuplicates(subset=['start_time'])

    # write time table to parquet files partitioned by year and month
    table_path = os.path.join(output_data, "time")

    time_table.write.mode('overwrite') \
        .partitionBy("year", "month") \
        .parquet(table_path)

    # read in song data to use for songplays table
    table_path = os.path.join(output_data, "songs")
    song_df = spark.read.parquet(table_path)

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df,  df.song == song_df.title) \
        .withColumn('songplay_id', monotonically_increasing_id()) \
        .withColumn('year', year('timestamp')) \
        .withColumn('month', month('timestamp')) \
        .select(
            'songplay_id', 
            col('ts').alias('start_time'), 
            col('userId').alias('user_id'), 
            'level', 
            'song_id', 
            'artist_id', 
            col('sessionId').alias('session_id'), 
            'location', 
            col('userAgent').alias('user_agent'), 
            'year', 
            'month')

    # write songplays table to parquet files partitioned by year and month
    table_path = os.path.join(output_data, "songplays")

    songplays_table.write.mode('overwrite') \
        .partitionBy("year", "month") \
        .parquet(table_path)


def main():
    """
    Start the ETL process
    """

    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lake-sjames/data-lake/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
