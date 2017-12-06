import sys
import shutil
from pyspark.sql import SparkSession, types

spark = SparkSession.builder.appName('Weather ETL - Clean Weather Data').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.1' # make sure we have Spark 2.1+

def main(in_directory, out_path, tmpdir):
    directory = tmpdir
    schema_file = open('schema')
    schema_lines = [i.strip() for i in schema_file.readlines()]
    schema = types.StructType([types.StructField(i, types.StringType(), False) for i in schema_lines])
    schema_file.close()
    
    weather = spark.read.csv(directory, schema=schema)#.withColumn('filename', functions.input_file_name())
    weather = weather[weather['Weather'] != 'NA']
    weather_columns = [i for i in weather.schema.names if 'Flag' not in i]
    weather = weather.select(
        weather_columns
    )
    weather.show()
    with open('schema', 'w+') as schema_out:
        schema_out.writelines([i + '\n' for i in weather_columns])


    weather.write.csv(out_path, mode='overwrite')

    shutil.rmtree(directory) #remove tempdir

if __name__=='__main__':
    in_directory = sys.argv[1] # should be yvr-weather
    out_path = sys.argv[2] # cleaned-weather
    tmpdir = sys.argv[3]
    main(in_directory, out_path, tmpdir)
