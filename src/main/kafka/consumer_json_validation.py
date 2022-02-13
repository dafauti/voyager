# bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2  /home/dafauti/IdeaProjects/FreeLancing/kafka/consumer_json_validation.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, to_json
from pyspark.sql.types import StructType, StringType
from schema_template import student_Schema
import kafka_connector

#validating_udf = udf(validator.validating)
if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Read json_validation topic") \
        .master("local[1]") \
        .getOrCreate()
    spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", True)
    schema = StructType().add("name", StringType())
    kafka_ctx=kafka_connector.KafkaConnector("json_testing",student_Schema)

    streaming_df=kafka_ctx.json_read(spark,schema)
    streaming_df.printSchema()
    validate_df=kafka_ctx.validation_upstream(streaming_df)
    validate_df.printSchema()
    validate_df.writeStream.format("console").start().awaitTermination()

    ##writng
    true_df=validate_df.filter(col("flag") == "true")
    kafka_ctx.write(true_df) ##
    false_df=validate_df.filter(col("flag") == "false")
    kafka_ctx.write(false_df)
'''
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "json_testing") \
        .option("startingOffsets", "earliest") \
        .load()
    

    kafka_df.printSchema()
    print("show the kafka message###########")
    df = kafka_df.select( \
        col("key").cast("string"), \
        from_json(col("value").cast("string"), schema).alias("input") \
    ).withColumn("input", to_json("input")).withColumn("flag", validating_udf("input"))

    #df.filter(col("flag") == "false").writeStream.format("console").start()
    #df.filter(col("flag") == "true").writeStream.format("console").start().awaitTermination()

    df.filter(col("flag") == "false").writeStream.format("csv") \
        .option("checkpointLocation", "/home/dafauti/Downloads/kafka_output_false") \
        .option("path", "/home/dafauti/Downloads/kafka_output") \
        .option("startingOffsets", "earliest") \
        .option("truncate", "false") \
        .start()

    df.filter(col("flag") == "true").writeStream.format("csv") \
        .option("checkpointLocation", "/home/dafauti/Downloads/kafka_output1") \
        .option("path", "/home/dafauti/Downloads/kafka_output_true") \
        .option("startingOffsets", "earliest") \
        .option("truncate", "false") \
        .start() \
        .awaitTermination()

    df.filter(col("flag") == "true").writeStream \
                            .format("delta") \
                            .outputMode("append") \
                            .option("checkpointLocation", "/home/dafauti/Downloads/kafka_output") \
                            .start("/delta/events")

df.writeStream.format("csv") \
        .option("checkpointLocation", "/home/dafauti/Downloads/kafka_output1") \
        .option("path", "/home/dafauti/Downloads/kafka_output") \
        .option("startingOffsets", "earliest") \
        .option("truncate", "false") \
        .start()

writeStream.queryName("aggregates").format("memory").start()
    spark.sql("select * from aggregates").show()
        
    # df=kafka_df.selectExpr("CAST(value AS STRING)")

    # df.writeStream.format("console").outputMode("append").start()
'''
