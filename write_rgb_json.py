import sys
import cv2
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

in_directory = sys.argv[1] # should be katkam-scaled
out_directory = sys.argv[2] # should be katkam-rgb-json

def path_to_time(path):
    timestamp = os.path.splitext(path)[0][-14:]
    #2017-05-01 16:00
    return "{}-{}-{} {}:00".format(timestamp[-14:-10], timestamp[-10:-8], timestamp[-8:-6], timestamp[-6:-4] )

def main():
    images = []
    #df = spark.createDataFrame([], schema)
    try:
        os.makedirs(os.path.dirname('{}/'.format('katkam-rgb-json')))
    except Exception as e:
        print(e)

    # Read images from katkam-scaled folder, write to json and then read into spark -> avoids memory issues
    for filename in glob.glob('{}/*.jpg'.format(in_directory)):
        img = cv2.imread(filename).flatten().tolist()
        with open((out_directory + '/{}').format(os.path.splitext(filename)[0][-21:]), 'w') as fp:
            json.dump({'time':path_to_time(filename), 'image': img}, fp)
        #images.append(img_row)
        #img_row = Row(time=path_to_time(filename), image=img)

        #df = df.union(img_row)

    #df = spark.createDataFrame(images)


if __name__=='__main__':
    main()