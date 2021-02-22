import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# TODO Create a schema for incoming resources
schema = StructType([
    StructField('crime_id', StringType(), True),
    StructField('original_crime_type_name', StringType(), True),
    StructField('report_date', DateType(), True),
    StructField('call_date', DateType(), True),
    StructField('offense_date', StringType(), True),
    StructField('call_time', StringType(), True),
    StructField('call_date_time', TimestampType(), True),
    StructField('disposition', StringType(), True),
    StructField('address', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True),
    StructField('agency_id', StringType(), True),
    StructField('address_type', StringType(), True),
    StructField('common_location', StringType(), True)
])


def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:3456") \
        .option("subscribe", "police.service.calls") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .option("maxRatePerPartition", 1000) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("cast(value as string)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select('original_crime_type_name', 'disposition', 'call_date_time').distinct()

    distinct_table.printSchema()
    #query=distinct_table.writeStream.outputMode("append").format("console").start()

    # count the number of original crime type
    #agg_df = distinct_table.count()
    #AM - use watermark according to https://knowledge.udacity.com/questions/63483
    agg_df = distinct_table \
             .select(psf.col("original_crime_type_name"), psf.col("call_date_time"), psf.col("disposition"))\
             .withWatermark("call_date_time", "10 minutes") \
             .groupBy(psf.window(distinct_table.call_date_time, "10 minutes", "2 minutes"), distinct_table.original_crime_type_name, distinct_table.disposition) \
             .count()

    agg_df.printSchema()
    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .start()

    #agg_df.printSchema()
    # TODO attach a ProgressReporter
    #query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True)

    radio_code_df.printSchema()
    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    radio_code_df.printSchema()
    # TODO join on disposition column
    join_query = agg_df \
                 .join(radio_code_df, agg_df.disposition == radio_code_df.disposition, 'inner') \
                 .writeStream \
                 .outputMode("complete") \
                 .format("console") \
                 .start()
                #.join(radio_code_df, "disposition") \
    
    join_query.awaitTermination()
    
if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", 3000) \
        .getOrCreate()

    #spark.sparkContext.setLogLevel("WARN") #done this to reduce INFO logs
    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
