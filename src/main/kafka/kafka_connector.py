import logging
from pyspark.sql import DataFrame

from pyspark.sql.types import StringType, StructType, IntegerType, MapType
from pyspark.sql.functions import from_json, col
from configparser import ConfigParser
import pyspark.sql.functions as fn
from pyspark.sql.functions import from_json, col, udf, to_json
import json
import jsonschema
from jsonschema import validate
import xmlschema
from lxml import etree, objectify
from lxml.etree import XMLSyntaxError
import xml.etree.ElementTree as ET

import xmltodict


class KafkaConnector:
    def __init__(self, topic, template_schema) -> None:
        self.topic = topic
        self.template_schema = template_schema

    def json_read(self, spark_streaming_ctx, schema) -> DataFrame:
        return spark_streaming_ctx \
            .readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "earliest") \
            .load() \
            .select(col("topic").cast("string"), \
                    col("partition").cast("string"), \
                    col("offset").cast("string"), \
                    col("timestampType").cast("string"), \
                    col("key").cast("string"), \
                    (from_json(col("value").cast("string"), schema)).alias("subrequest"), \
                    col("value").cast("string").alias("subrequest1"))

    def xml_read(self, spark_streaming_ctx, schema) -> DataFrame:
        return spark_streaming_ctx \
            .readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "earliest") \
            .load() \
            .select(col("topic").cast("string"), \
                    col("partition").cast("string"), \
                    col("offset").cast("string"), \
                    col("timestampType").cast("string"), \
                    col("key").cast("string"), \
                    col("value").cast("string").alias("subrequest")
                    )
            #.selectExpr("xpath_int(CAST(value as STRING),'/OFX/TSVERMSGSRSV1/TSVTWNSELECTTRNRS/STATUS/CODE/text()') as status_code")
    def validating(self, streaming_df):
        try:
            print(streaming_df)
            validate(json.loads(streaming_df),
                     schema=self.template_schema)  # validate is a jsonschema function, where we passing the expected json and input json
            return True
        except jsonschema.exceptions.ValidationError as err:
            return False

    def validation_upstream(self, streaming_df) -> DataFrame:
        #validating_udf = fn.udf(lambda x: True if validate(json.loads(x), schema=self.template_schema) is None else False)

        validating_udf = fn.udf(self.validating)
        return streaming_df.withColumn("subrequest", to_json("subrequest")).withColumn("flag", validating_udf("subrequest"))

    def xml_validator(self, some_xml_string):
        try:
            xmlschema.validate(some_xml_string, self.template_schema)
            return True
        except xmlschema.exceptions.XMLSchemaException as error:
            return False

    def validation_xml_upstream(self, streaming_df) -> DataFrame:
        validating_udf = fn.udf(self.xml_validator)
        # streaming_df.writeStream.format("console").start().awaitTermination()
        return streaming_df.withColumn("flag", validating_udf("subrequest"))

    def xml_to_json(self, streaming_df) -> DataFrame:
        transform_json = fn.udf(lambda x: json.dumps(xmltodict.parse(x)))
        status_code=fn.udf(lambda x:json.loads(x)["OFX"]["TSVERMSGSRSV1"]["TSVTWNSELECTTRNRS"]["STATUS"]["CODE"])
        return streaming_df.withColumn("json_data", transform_json("subrequest")).withColumn("status_dode",status_code('json_data'))

    def fetch_employee_details(self, json_string):
        try:
            #print(json.loads(json_string)["OFX"]["TSVERMSGSRSV1"]["TSVTWNSELECTTRNRS"]["TSVTWNSELECTRS"]["TSVRESPONSE_V100"])
            df=json.loads(json_string)["OFX"]["TSVERMSGSRSV1"]["TSVTWNSELECTTRNRS"]["TSVTWNSELECTRS"]["TSVRESPONSE_V100"]
            print(type(df))
            str=''
            for item in df:
                str = str +json.dumps(item)+"#@#"
            str = str[:-3] if str else str
            return str
        except jsonschema.exceptions.ValidationError as err:
            print(err)
            return False

    def json_write(self, streaming_df) -> DataFrame:
        employee_df=fn.udf(self.fetch_employee_details)
        return  streaming_df.filter(col('status_dode') =='0').withColumn("bronze",employee_df("json_data")).withColumn("bronze", fn.explode(
            fn.split("bronze", "#@#")))