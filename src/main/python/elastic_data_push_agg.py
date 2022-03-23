from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch_dsl import *
from elasticsearch_dsl import analysis
from elasticsearch_dsl.connections import connections as es_connections
import csv
import warnings
warnings.filterwarnings('ignore')

es_connections.create_connection(hosts=['https://esa.wsdc-tlmy-stores-prd01.carbon.gmail.com:443'],timeout=20)
es=Elasticsearch(['https://esa.wsdc-tlmy-stores-prd01.carbon.gmail.com:443'],api_key='TTdGTEpYOEIyaUc5LWdqeHdHV1I6UnJEWHdNaUZSemFjQWJobWJRZkh6Zw==',use_ssl=True, verify_certs=False)

filename = "/home/svcmapprdrw/agg_quality/aggregate.csv"
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

    class Index:
        name="dpp-canada-aggregation-quality-prod"
        using=es

def indexdata():
    if es.indices.exists(index='dpp-canada-aggregation-quality-prod'):
        try:
            es.indices.delete(index='dpp-canada-aggregation-quality-prod')
        except:
            pass
    GenericAggregationIndex.init()

    # reading csv file
    with open(filename, 'r') as csvfile:
        # creating a csv reader object
        csvreader = csv.reader(csvfile)

        # extracting field names through first row
        fields = next(csvreader)

        # extracting each data row one by one
        for row in csvreader:
            rows.append(row)
    for row in rows:
        try:
            col=row[0].split('|')
            #print(col)
            obj = GenericAggregationIndex(fiscalYear = col[0],fiscalMonth=col[1],allocationValue=col[2],rank=col[3],loadDate=col[4],banner=col[5],dataSet=col[6])
            obj.save()
        except Exception as e:
            print(str(e))

if __name__=='__main__':
    try:
        indexdata()
    except Exception as e:
        print(str(e))
