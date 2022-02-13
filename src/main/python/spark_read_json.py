#from pyspark.sql.types import *
#from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, upper, udf, lit, regexp_replace, explode
from pyspark.sql.types import StructType, DoubleType, StringType, NullType, IntegerType, ArrayType


spark = SparkSession.builder \
       .appName("json validation") \
       .getOrCreate()
sc = spark.sparkContext

nested_json_file="/home/dafauti/Downloads/sub_task.json"
df = spark.read.json(path=nested_json_file,multiLine=True)
#df.show(10,False)
#df.printSchema()

#df.select(['orderItem.id']).show()

def read_nested_json(df):
       column_list=[]
       for column_name in df.schema.names:
              print("outside isinstance loop:"+ column_name)
              # checking column type is ArrayType
              if isinstance(df.schema[column_name].dataType, ArrayType):
                     print("inside isinstance loop of ArrayType"+ column_name)
                     df= df.withColumn(column_name, explode(column_name).alias(column_name))
                     column_list.append(column_name)
              elif isinstance(df.schema[column_name].dataType, StructType):
                     print("inside isinstance loop of ArrayType"+ column_name)
                     for field in df.schema[column_name].dataType.fields:
                            column_list.append(col(column_name + "." + field.name).alias(column_name+"_"+ field.name))
              else:
                     column_list.append(column_name)
       df = df.select(column_list)
       return  df

read_nested_json_flag=True
while read_nested_json_flag:
       print("Reading Nested JSON file......")
       df = read_nested_json(df)
       #df.show(100,False)
       read_nested_json_flag=False

       for column_name in df.schema.names:
              if isinstance(df.schema[column_name].dataType,ArrayType):
                     read_nested_json_flag=True
              elif isinstance(df.schema[column_name].dataType,StructType):
                     read_nested_json_flag=True
              else:
                     read_nested_json_flag=False

df.show(100,False)