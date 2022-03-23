import argparse
from pyspark.sql import SparkSession

def indexdata(sql_query,csv_path):
    spark = SparkSession.builder.appName('generate_csv_file').enableHiveSupport().getOrCreate()
    spark.sql(sql_query).coalesce(1).write.option("delimiter",'|').mode('overwrite').option("header","true").csv(csv_path)

if __name__=='__main__':
    try:
        parser=argparse.ArgumentParser()
        parser.add_argument("sql_query")
        parser.add_argument("csv_path")
        args=parser.parse_args()
        indexdata(args.sql_query,args.csv_path)
    except Exception as e:
        print(str(e))
