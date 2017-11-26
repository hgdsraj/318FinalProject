import sys
# from pyspark.sql import SparkSession, functions, types
import numpy as np
import cv2
import shutil
from skimage.color import rgb2grey
from pyspark.sql import SparkSession, functions, types, Row
import json
import os
import glob

schema = types.StructType([
    types.StructField('Date/Time',types.StringType(),True),
    types.StructField("image",types.ArrayType(types.LongType()),False)
])
#https://stackoverflow.com/questions/31477598/how-to-create-an-empty-dataframe-with-a-specified-schema

# or df = sc.parallelize([]).toDF(schema)

# Spark < 2.0
# sqlContext.createDataFrame([], schema)

spark = SparkSession.builder.appName('Weather Image Classifier').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+

def main():
    schema_file = open('schema')
    schema_lines = [i.strip() for i in schema_file.readlines()]
    schema = types.StructType([types.StructField(i, types.StringType(), False) for i in schema_lines])
    schema_file.close()
    weather = spark.read.csv(sys.argv[3], schema=schema)  # .withColumn('filename', functions.input_file_name())
    
    df = spark.read.json('cleaned-katkam')


    df.show()

if __name__=='__main__':
    main()