# bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2  /home/dafauti/IdeaProjects/FreeLancing/kafka/consumer_json_validation.py
#import xml.etree.ElementTree as ET

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, to_json
from pyspark.sql.types import StructType, StringType
from schema_template import student_Schema,xml_template
import kafka_connector

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Read xml_validation topic") \
        .master("local[1]") \
        .getOrCreate()
    spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", True)
    schema = StructType().add("name", StringType())
    kafka_ctx=kafka_connector.KafkaConnector("xml_testing",xml_template,"[\"OFX\"][\"TSVERMSGSRSV1\"][\"TSVTWNSELECTTRNRS\"][\"TSVTWNSELECTRS\"][\"TSVRESPONSE_V100\"]")

    streaming_df=kafka_ctx.xml_read(spark,schema)
    validate_df=kafka_ctx.validation_xml_upstream(streaming_df)
    json_df=kafka_ctx.xml_to_json(validate_df)
    json_df.printSchema()

    json_df=kafka_ctx.fetch_employee_response(json_df)
    # mapping
    # join subrequest data based on subrequest ID + appending

    json_df.writeStream.format("console").option("truncate", False).start().awaitTermination()
    #result_json = json.dumps(xmlDict)
    # self.logger.log(result_json)


