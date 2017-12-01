import sys
import shutil
from pyspark.sql import SparkSession, types

schema = types.StructType([
    types.StructField('time', types.StringType(),True),
    types.StructField("image", types.ArrayType(types.LongType()),False)
])

spark = SparkSession.builder.appName('Weather ETL - Clean Image Data').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+

katkam_in_directory = sys.argv[1] # should be katkam-rgb-json
weather_in_directory = sys.argv[2] # should be cleaned-weather
out_directory = sys.argv[3] # should be cleaned-katkam-rgb

def main():
    schema_file = open('schema')
    schema_lines = [i.strip() for i in schema_file.readlines()]
    schema = types.StructType([types.StructField(i, types.StringType(), False) for i in schema_lines])
    schema_file.close()
    weather = spark.read.csv(weather_in_directory, schema=schema)  # .withColumn('filename', functions.input_file_name())

    df = spark.read.json(katkam_in_directory)
    df.show()

    df = df.select(df['time'].alias('Date/Time'), df['image'])
    df = weather.join(df, 'Date/Time')
    df = df.select(df['Date/Time'], df['image'])
    print(df.schema)
    df.show()

    df.write.json(out_directory, mode='overwrite')

    shutil.rmtree('katkam-rgb-json') #remove tempdir

if __name__=='__main__':
    main()