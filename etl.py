import configparser
import os
from pyspark.sql import SparkSession
import datetime as dt
from pyspark.sql.functions import udf, hour, dayofmonth, dayofweek, weekofyear, month, year, monotonically_increasing_id
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates Spark Session.

    Returns:
        spark: Spark Session.

    """

    print("Starting SparkSession.")
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    print("DONE.")
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads and processes song data.

    Args:
        spark: Spark Session.
        input_data: input data directory.
        output_data: output data directory.

    """

    # get filepath to song data file
    song_data = input_data + "song_data/A/A/A/*.json"
    
    # read song data file
    print("\nReading song data.")
    dfSongData = spark.read.json(song_data)
    print("DONE.")

    # extract columns to create songs table
    dfSongs = dfSongData.select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    print("\nWriting song dataframe.")
    dfSongs.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs.parquet', mode='overwrite')
    print("DONE.")

    # extract columns to create artists table
    dfArtists = dfSongData.selectExpr('artist_id', 'artist_name as name', 'artist_location as location', \
        'artist_latitude as latitude', 'artist_longitude as longitude')
    
    # write artists table to parquet files
    print("\nWriting artist dataframe.")
    dfArtists.write.parquet(output_data + 'artists.parquet', mode='overwrite')
    print("DONE.")


def process_log_data(spark, input_data, output_data):
    """
    Reads and processes log data.

    Args:
        spark: Spark Session.
        input_data: input data directory.
        output_data: output data directory.

    """


    # get filepath to log data file
    log_data = input_data + "log-data/2018/11/*.json"

    # get filepath to song data file
    song_data = input_data + "song_data/A/A/A/*.json"

    # read log data file
    print("\nReading log data.")
    dfLogData = spark.read.json(log_data)
    print("DONE.")

    # filter by actions for song plays
    dfLogData = dfLogData.filter('page = "NextSong"')

    # extract columns for users table    
    dfUsers = dfLogData.selectExpr('userId as user_id', 'firstName as first_name', \
        'lastName as last_name', 'gender', 'level').distinct()
    
    # write users table to parquet files
    print("\nWriting user dataframe.")
    dfUsers.write.parquet(output_data + 'users.parquet', mode='overwrite')
    print("DONE.")

    # define udf to get datetime
    @udf(TimestampType())
    def get_datetime(ts):
        datetime_time = dt.datetime.fromtimestamp(ts/1000)
        return datetime_time
    
    # create datetime column from original timestamp column
    dfLogData = dfLogData.withColumn('datetime', get_datetime('ts'))
    
    # extract columns to create time table
    dfTime = dfLogData.select(
        dfLogData.datetime.alias('start_time'),
        hour(dfLogData.datetime).alias('hour'),
        dayofmonth(dfLogData.datetime).alias('day'),
        weekofyear(dfLogData.datetime).alias('week'),
        month(dfLogData.datetime).alias('month'),
        year(dfLogData.datetime).alias('year'),
        dayofweek(dfLogData.datetime).alias('weekday')).distinct()

    # write users table to parquet files
    print("\nWriting time dataframe.")
    dfTime.write.partitionBy('year', 'month').parquet(output_data + 'time.parquet', mode='overwrite')
    print("DONE.")
    
    # build songplays table
    dfSongPlays = dfLogData.selectExpr(
        'datetime as start_time',
        'userId as user_id',
        'level',
        'song',
        'artist',
        'sessionId as session_id',
        'location',
        'userAgent as user_agent')

    # read song data file
    dfSongData = spark.read.json(song_data)
    dfSongDataSong = dfSongData.select('song_id', 'title')
    dfSongDataArtist = dfSongData.select('artist_id', 'artist_name')

    # join with song data
    dfSongPlays = dfSongPlays.join(dfSongDataSong, dfSongPlays.song == dfSongDataSong.title).drop('song', 'title')

    # join with artist data
    dfSongPlays = dfSongPlays.join(dfSongDataArtist, dfSongPlays.artist == dfSongDataArtist.artist_name).drop('artist', 'artist_name')

    # add id
    dfSongPlays = dfSongPlays.withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    print("\nWriting time dataframe.")
    dfSongPlays.write.partitionBy('year', 'month').parquet(output_data + 'songplays.parquet', mode='overwrite')
    print("DONE.")


def main():
    """
    Main function.

    """

    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lakes-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    spark.stop()
    print("\nSpark session closed.")



if __name__ == "__main__":
    main()
