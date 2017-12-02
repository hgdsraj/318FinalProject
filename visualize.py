import sys
from pyspark import SparkContext
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import SparkSession, functions, types,

schema = types.StructType([
    types.StructField('Date/Time',types.StringType(),True),
    types.StructField("image",VectorUDT(),False)
])
#https://stackoverflow.com/questions/31477598/how-to-create-an-empty-dataframe-with-a-specified-schema

# or df = sc.parallelize([]).toDF(schema)

# Spark < 2.0
# sqlContext.createDataFrame([], schema)

katkam_in_directory = sys.argv[1] # should be either cleaned-katkam-grayscale or cleaned-katkam-rgb
weather_in_directory = sys.argv[2] # should be cleaned-weather
# out_directory = sys.argv[3] # will decide later what output will be, will probably be predictions

spark = SparkSession.builder.appName('Weather Image Classifier - Data Analysis').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+

cleaned_katkam = sys.argv[1] # should be cleaned-katkam-<rgb/greyscale>
cleaned_weather = sys.argv[2] # should be cleaned-weather

# All the labels:
## ['Cloudy', 'Rain Showers', 'Rain', 'Snow', 'Fog', 'Moderate Rain', 'Drizzle,Fog', 'Mostly Cloudy', 'Clear', 'Snow Showers', 'Mainly Clear', 'Rain,Drizzle', 'Drizzle']

def main():
    df = spark.read.json(katkam_in_directory)
    schema_file = open('schema')
    schema_lines = [i.strip() for i in schema_file.readlines()]
    schema = types.StructType([types.StructField(i, types.StringType(), False) for i in schema_lines])
    schema_file.close()
    weather = spark.read.csv(weather_in_directory, schema=schema)#.withColumn('filename', functions.input_file_name())

    df = df.join(weather, 'Date/Time')
    df.show()

if __name__ == '__main__':
    main()