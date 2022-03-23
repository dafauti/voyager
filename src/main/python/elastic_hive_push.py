from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch_dsl import *
from elasticsearch_dsl import analysis
from elasticsearch_dsl.connections import connections as es_connections
import csv
import pyspark
from pyspark.sql import SparkSession

es_connections.create_connection(hosts=['https://esa.wsdc-tlmy-stores-prd01.carbon.gmail.com:443'],timeout=20)
es=Elasticsearch(['https://esa.wsdc-tlmy-stores-prd01.carbon.gmail.com:443'],api_key='TTdGTEpYOEIyaUc5LWdqeHdHV1I6UnJEWHdNaUZSemFjQWJobWJRZkh6Zw==',use_ssl=True, verify_certs=False)

#filename = "/Users/3551341/Documents/aggregation_validation.csv"
# initializing the titles and rows list
fields = []
rows = []
class GenericAggregationIndex(Document):
    fiscalYear=Integer()
    fiscalMonth=Integer()
    allocationValue=Double()
    rank=Integer()
    loadDate=Keyword(normalizer=analysis.normalizer("lowercaseNorm", filter=['lowercase','asciifolding']))
    banner=Keyword(normalizer=analysis.normalizer("lowercaseNorm", filter=['lowercase','asciifolding']))
    dataSet=Keyword(normalizer=analysis.normalizer("lowercaseNorm", filter=['lowercase','asciifolding']))
    #dpp-canada-aggregation-quality-qa
    #dpp-canada-combined-quality-qa
    class Index:
        #name="aggregation-quality"
        name="dpp-canada-aggregation-quality-prod"
        using=es

def indexdata():
    spark = SparkSession.builder.appName('elastic Search').enableHiveSupport().getOrCreate()
    #spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    ##spark.conf.set ("spark.sql.hive.convertMetastoreOrc","false")
    data=spark.sql("select coalesce(fiscal_year,0) as fiscal_year,coalesce(fiscal_month,0) as fiscal_month,coalesce(allocation_value,0) as allocation_value,rank,load_date,banner,dataset from dpp_canada.dpp_data_quality_agg where load_date in (select distinct load_date from dpp_canada.dpp_data_quality_agg order by load_date desc limit 2)")
    if es.indices.exists(index='dpp-canada-aggregation-quality-prod'):
        try:
            es.indices.delete(index='dpp-canada-aggregation-quality-prod')
        except:
            pass
    GenericAggregationIndex.init()
    data.write.format("org.elasticsearch.spark.sql"). \
           option("es.resource", "dpp-canada-aggregation-quality-prod"). \
           option("es.nodes", "https://esa.wsdc-tlmy-stores-prd01.carbon.gmail.com"). \
           option("es.port", "443"). \
           mode("append").save()

if __name__=='__main__':
    try:
        indexdata()
    except Exception as e:
        print(str(e))
