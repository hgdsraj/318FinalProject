import sys
# from pyspark.sql import SparkSession, functions, types
import numpy as np
import cv2
from skimage.color import rgb2grey
from pyspark.sql import SparkSession, functions, types, Row

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
def path_to_time(path):
    timestamp = os.path.splitext(path)[0][-14:]
    #2017-05-01 16:00
    return "{}-{}-{} {}:00".format(timestamp[-14:-10], timestamp[-10:-8], timestamp[-8:-6], timestamp[-6:-4] )

def main():
    images = []
    #https://stackoverflow.com/questions/952914/making-a-flat-list-out-of-list-of-lists-in-python
    flatten = lambda l: [item for sublist in l for item in sublist]
    schema_file = open('schema')
    schema_lines = [i.strip() for i in schema_file.readlines()]
    schema = types.StructType([types.StructField(i, types.StringType(), False) for i in schema_lines])
    schema_file.close()
    weather = spark.read.csv(sys.argv[3], schema=schema)#.withColumn('filename', functions.input_file_name())

    # Read a single image from katkam-scaled folder, use spark later
    for filename in glob.glob('{}/*.jpg'.format(sys.argv[1])):
        img = rgb2grey(cv2.imread(filename)).tolist()
        images.append(Row(time=path_to_time(filename), image=flatten(img)))
    df = spark.createDataFrame(images)
    df.show()

    df = df.select(df['time'].alias('Date/Time'), df['image'])
    df = weather.join(df, 'Date/Time')
    df = df.select(df['Date/Time'], df['image'])
    df.show()

    df.write.json(sys.argv[2], mode='overwrite')


    print("wpow")


if __name__=='__main__':
    main()