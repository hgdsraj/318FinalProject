import sys
import os
from pyspark.sql import SparkSession, types, functions
from pyspark.sql.types import DoubleType, IntegerType, LongType, FloatType, ArrayType,DataType
from functools import reduce
spark = SparkSession.builder.appName('Weather Image Classifier').getOrCreate()
import glob
import re
assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+

tide_in_directory = sys.argv[1] # should be tide-foldeer
out_directory = sys.argv[2] # should be tide-cleaned

def path_to_time(path):
    timestamp = os.path.splitext(path)[0][-14:]
    #2017-05-01 16:00
    return "{}-{}-{} {}:00".format(timestamp[-14:-10], timestamp[-10:-8], timestamp[-8:-6], timestamp[-6:-4] )

def main():
    try:
        os.makedirs(os.path.dirname('{}/'.format(out_directory)))
    except Exception as e:
        print(e)

    date_re = r'\d\d\d\d-\d\d-\d\d \d\d:00'

    # Read images from katkam-scaled folder, write to json and then read into spark -> avoids memory issues
    in_folder = glob.glob('{}/*.csv'.format(tide_in_directory))
    count = len(in_folder)
    dataframes = [spark.read.csv(filename) for filename in in_folder]
    df = reduce((lambda x, y: x.union(y)), dataframes)
    df = df[df['_c1'].rlike(date_re)]
    df = df[df['_c0'] != 'STATION_ID']
    df = df.select(df['_c1'].alias('Date/Time'), ((df['_c3'] + df['_c4']) / 2).cast(types.LongType()).alias('label'))
    df.write.json(out_directory, mode='overwrite')
if __name__=='__main__':
    main()