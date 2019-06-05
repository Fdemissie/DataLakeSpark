import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import *
import  pyspark.sql.functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title","artist_id", "year", "duration").dropDuplicates()
    songs_table.limit(5).toPandas()
    
    # write songs table to parquet files partitioned by year and artist
    songsParquetPath = os.path.join(output_data, "songs.parquet")
    songs_table.write.partitionBy("year", "artist_id").parquet(songsParquetPath)
    songsParquetFile = spark.read.parquet(songsParquetPath)
    songsParquetFile.createOrReplaceTempView("songsParquetFile")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").dropDuplicates()
    artists_table.limit(5).toPandas()  
    
    # write artists table to parquet files
   
    artistParquetPath = os.path.join(output_data, "artists.parquet")
    artists_table.write.parquet(artistParquetPath)
    artistsParquetFile = spark.read.parquet(artistParquetPath)
    artistsParquetFile.createOrReplaceTempView("artistsParquetFile")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")
      
    # read log data file
    df = spark.read.json(log_data)
    
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
   

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").dropDuplicates()
     
    
    # write users table to parquet files
    usersParquetPath = os.path.join(output_data, "users.parquet") 
    users_table.write.parquet(usersParquetPath)
    usersParquetFile = spark.read.parquet(usersParquetPath)
    usersParquetFile.createOrReplaceTempView("usersParquetFile")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("datetime", get_timestamp(df.ts))
    df = df.withColumn("year", F.year("datetime"))\
       .withColumn("month", F.month("datetime"))\
       .withColumn("day", F.dayofweek("datetime"))\
       .withColumn("hour", F.hour("datetime"))\
       .withColumn("minute", F.minute("datetime"))\
       .withColumn("second", F.second("datetime"))\
       .withColumn("weekday", F.dayofweek("datetime"))
    
    # create datetime column from original timestamp column
    # get_datetime = udf()
    # df = 
    
    # extract columns to create time table
    time_table = df.select("ts", "year", "month", "day", "hour", "minute", "second", "weekday" ).dropDuplicates()
   
    
    # write time table to parquet files partitioned by year and month
    
    timeParquetPath = os.path.join(output_data, "time.parquet")
    time_table.write.partitionBy("year", "month").parquet(timeParquetPath)
    timeParquetFile = spark.read.parquet(timeParquetPath)
    timeParquetFile.createOrReplaceTempView("timeParquetFile")

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    song_df = spark.read.json(song_data)
    
    
    # extract columns from joined song and log datasets to create songplays table 
    df1 = df.alias('df1')
    df2 = song_df.alias('df2')
    songplays_table = df1.join(df2, df1.artist == df2.artist_name)\
                         .select("ts", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent", "df1.year", "month")\
                         .dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    
    songPlaysParquetPath = os.path.join(output_data, "songplay.parquet")
    songplays_table.write.partitionBy("year", "month").parquet(songPlaysParquetPath)
    songplayParquetFile = spark.read.parquet(songPlaysParquetPath)
    songplayParquetFile.createOrReplaceTempView("songplayParquetFile")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://fisseha-bucket-dend/parquet/"
    
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
