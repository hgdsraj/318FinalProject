import sys
import shutil
from pyspark.sql import SparkSession, types, functions
from pyspark.sql.types import DoubleType, IntegerType, LongType, FloatType, ArrayType,DataType

spark = SparkSession.builder.appName('Weather Image Classifier').config('maxColumns', 100000)
spark= spark.getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+

katkam_in_directory = sys.argv[1] # should be katkam-<rgb/greyscaled>-json
weather_in_directory = sys.argv[2] # should be cleaned-weather
out_directory = sys.argv[3] # should be cleaned-katkam-<rgb/greyscale>
spark.read.option('maxColumns', 100000)
spark.conf.set('maxColumns', 100000)

def main():
    df = spark.read.option('maxColumns', 100000).json(katkam_in_directory)

    schema_file = open('schema')
    schema_lines = [i.strip() for i in schema_file.readlines()]
    schema = types.StructType([types.StructField(i, types.StringType(), False) for i in schema_lines])
    schema_file.close()
    weather = spark.read.option('maxColumns', 100000).csv(weather_in_directory, schema=schema)
    def join_other_columns(x, *args):
        return x + [int(float(i)) for i in args]

    with_other_columns = functions.UserDefinedFunction(lambda x, *args: join_other_columns(x, *args), ArrayType(LongType()))
    df = df.select(df['time'].alias('Date/Time'), df['image'])
    df = weather.join(df, 'Date/Time')
    df = df.select(df['Date/Time'], with_other_columns(df['image'], df['Rel Hum (%)'],
                                                       df['Temp (°C)'], df['Wind Dir (10s deg)'],
                                                       df['Wind Spd (km/h)'], df['Visibility (km)'],
                                                       df['Dew Point Temp (°C)']).alias('image'))
    df.write.json(out_directory, mode='overwrite')

    shutil.rmtree(katkam_in_directory) #remove tempdir


if __name__=='__main__':
    main()