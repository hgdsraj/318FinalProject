import sys
# from pyspark.sql import SparkSession, functions, types
import numpy as np
import cv2
import shutil
from skimage.color import rgb2grey
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType
import json
schema = StructType([
    StructField('Date/Time',StringType(),True),
    StructField("image",ArrayType(LongType()),False)
])
#https://stackoverflow.com/questions/31477598/how-to-create-an-empty-dataframe-with-a-specified-schema

# or df = sc.parallelize([]).toDF(schema)

# Spark < 2.0
# sqlContext.createDataFrame([], schema)

spark = SparkSession.builder.appName('Weather Image Classifier').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+

import os
import glob
'''
spark = SparkSession.builder.appName('weather classification').getOrCreate()

assert sys.version_info >= (3, 4)  # make sure we have Python 3.4+
assert spark.version >= '2.1'  # make sure we have Spark 2.1+

# Placeholder schema based on exercise 11
schema = types.StructType([
    types.StructField('score', types.LongType(), False),
    #types.StructField('score_hidden', types.BooleanType(), False),
    types.StructField('subreddit', types.StringType(), False),
])
'''

def main():
    df = spark.read.json('cleaned-katkam')
    df.show()

if __name__=='__main__':
    main()