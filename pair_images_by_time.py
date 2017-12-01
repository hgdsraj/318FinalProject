import sys
import numpy as np
import cv2
import shutil
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType
import json
import os
import glob
schema = StructType([
    StructField('time',StringType(),True),
    StructField("image",ArrayType(LongType()),False)
])
#https://stackoverflow.com/questions/31477598/how-to-create-an-empty-dataframe-with-a-specified-schema

# or df = sc.parallelize([]).toDF(schema)

# Spark < 2.0
# sqlContext.createDataFrame([], schema)

spark = SparkSession.builder.appName('Weather Image Classifier').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+

katkam_in_directory = sys.argv[1] # should be katkam-json
out_directory = sys.argv[2] # should be cleaned-katkam-grayscale
weather_in_directory = sys.argv[3] # should be cleaned-weather

def path_to_time(path):
    timestamp = os.path.splitext(path)[0][-14:]
    #2017-05-01 16:00
    return "{}-{}-{} {}:00".format(timestamp[-14:-10], timestamp[-10:-8], timestamp[-8:-6], timestamp[-6:-4] )

def main():
    #df = spark.createDataFrame(images)
    df = spark.read.json(katkam_in_directory)
    weather = spark.read.csv(weather_in_directory, schema=schema)#.withColumn('filename', functions.input_file_name())

    df = df.select(df['time'].alias('Date/Time'), df['image'])
    df = weather.join(df, 'Date/Time')
    df = df.select(df['Date/Time'], df['image'])

    df.write.json(out_directory, mode='overwrite')

    shutil.rmtree(katkam_in_directory) #remove tempdir




if __name__=='__main__':
    main()