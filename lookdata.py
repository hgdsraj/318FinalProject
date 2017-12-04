import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Weather Image Classifier').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+

def main():
    df = spark.read.json('weather-predictions')
    df.show()

if __name__=='__main__':
    main()