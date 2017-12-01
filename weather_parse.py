import sys
import os
import glob
import shutil
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('Weather ETL').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.1' # make sure we have Spark 2.1+

def remove_legend_create_temp(directory):
    tempdir = 'tempdir'

    try:
        os.makedirs(os.path.dirname('{}/'.format(tempdir)))
    except Exception as e:
        print(e)

    headers = None
    column_names = None

    for filename in glob.glob('{}/*.csv'.format(directory)):
        with open(filename) as f:
            tempf = open('{}/{}'.format(tempdir, os.path.basename(f.name)), 'w+')
            f_lines = f.readlines()
            tempf.writelines(f_lines[17:])
            headers = f_lines[0:16]
            column_names = f_lines[16]

    return headers, [i.strip()[1:-1] for i in column_names.split(',')], tempdir

def create_schema(column_names):
    # this is probably what we want, but it did not work:
    # date = {"Date/Time"}
    # numbers = {"Temp (C)","Dew Point Temp (C)","Rel Hum (%)","Wind Dir (10s deg)","Wind Spd (km/h)","Visibility (km)","Stn Press (kPa)","Hmdx","Wind Chill"}
    # #strings = {"Data Quality", "Wind Chill Flag","Temp Flag","Dew Point Temp Flag","Rel Hum Flag","Wind Dir Flag","Wind Spd Flag","Visibility Flag","Stn Press Flag","Hmdx Flag","Weather"}
    # typefields = []
    # for i in column_names:
    #     if i in date:
    #         typefields.append(types.StructField(i, types.DateType(), False))
    #     elif i in numbers:
    #         typefields.append(types.StructField(i, types.LongType(), False))
    #     else:
    #         typefields.append(types.StructField(i, types.StringType(), False))
    #
    # print(typefields)
    return types.StructType([types.StructField(i, types.StringType(), False) for i in column_names])
    #return types.StructType(typefields)



def main(in_directory, out_path):
    headers, column_names, directory = remove_legend_create_temp(in_directory)
    schema = create_schema(column_names)
    # print(headers)
    # print(schema)
    weather = spark.read.csv(directory, schema=schema)#.withColumn('filename', functions.input_file_name())
    # print(weather.schema)
    weather = weather[weather['Weather'] != 'NA']
    # weather.show()
    weather_columns = [i for i in weather.schema.names if 'Flag' not in i]
    # print(weather_columns)
    weather = weather.select(
        weather_columns
    )
    weather.show()


    weather.write.csv(out_path, mode='overwrite')
    with open('schema', 'w+') as schema_out:
        schema_out.writelines([i + '\n' for i in weather_columns])
    with open('header', 'w+') as header:
        header.writelines(headers)

    shutil.rmtree(directory) #remove tempdir

if __name__=='__main__':
    in_directory = sys.argv[1] # should be yvr-weather
    out_path = sys.argv[2] # cleaned-weather
    main(in_directory, out_path)
