import sys
# from pyspark.sql import SparkSession, functions, types
import numpy as np
import cv2

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
    image_path = 'katkam-scaled/katkam-20160605060000.jpg'
    img = cv2.imread(image_path)
    cv2.imshow('image', img)
    print(img)
    cv2.waitKey(0)
    cv2.destroyAllWindows()


if __name__=='__main__':
    main()