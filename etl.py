from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, dayofweek, concat
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format



def create_spark_session():
    """
    Description: This function creates the only spark session that would be required to run the script

    Arguments:
        None

    Returns:
        None
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_data(spark, input_data, output_data):
    """
    Description: This function loads all the data, extracts the required columns for the star schema and writes back \
                 to S3

    Arguments:
        spark : the spark session that would used to run all the spark statements
        input_data : this was created in the main function. It has the main folder that is common to all files that \
                     will be loaded from to avoid repiting the same folder name several times
        output_data : this was created in the main function. It has the main folder that is common to all files that \
                     will be written back to S3 to avoid repiting the same folder name several times
    
    Returns:
        None
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*'
    
    # read song data file
    df = spark.read.json(song_data)

    #extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration']).dropDuplicates()
    
    #write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + "song_table")

    #extract columns to create artists table
    artists_table = df.select(['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']).dropDuplicates()
    
    #write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artist_table")


    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*'

    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log = df_log.where(df_log.page == 'NextSong')

    # extract columns for users table    
    users_table = df_log.select(['userId','firstName','lastName','gender','level']).where(df_log.userId != "") \
                        .sort('ts').dropDuplicates(['userId','firstName','lastName'])
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "user_table")

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df = 
    
    # create datetime column from original timestamp column
    # get_datetime = udf()
    # df = 
    
    
    # extract columns to create time table
    time_table = df_log.select('ts').dropDuplicates() \
               .withColumn('ts', to_timestamp('ts')).select(col("ts").alias("start_time")) \
               .withColumn('hour', hour('start_time')) \
               .withColumn('day', dayofmonth('start_time')) \
               .withColumn('week', weekofyear('start_time')) \
               .withColumn('month', month('start_time')) \
               .withColumn('year', year('start_time')) \
               .withColumn('weekday', dayofweek('start_time'))
    
    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "time_table")

    # read in song data to use for songplays table
    # song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df_log.join(df, df_log.artist == df.artist_name, 'inner')\
                   .select(concat(df_log.sessionId, df_log.itemInSession).alias('songplay_id'),\
                            to_timestamp(df_log.ts).alias('start_time'),\
                            year(to_timestamp(df_log.ts)).alias('year'),\
                            month(to_timestamp(df_log.ts)).alias('month'),\
                   df_log.userId,df_log.level,df.song_id, df.artist_id,df_log.sessionId,\
                    df.artist_location.alias('location'),df_log.userAgent)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + "song_play_table")


def main():
    """
    Description: The main fucntion helps to bootstrap the whole script. It calls the function to create \
                 the spark session, create the required folder names to avoid repition, run the \
                 process_data function to process the data and write back to S3 and finally stop the spark session
                 
    Arguments:
        None
    
    Returns:
        None
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://olayemiodefunsho/schema/"
    process_data(spark, input_data, output_data)  
    spark.stop()
    

if __name__ == "__main__":
    main()

    
    
#def process_song_data(spark, input_data, output_data):

#def process_log_data(spark, input_data, output_data):

#def main():
#    spark = create_spark_session()
#    input_data = "s3a://udacity-dend/"
#    output_data = ""
    
#    process_song_data(spark, input_data, output_data)    
#    process_log_data(spark, input_data, output_data)


