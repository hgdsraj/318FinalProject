import sys
import os
import glob
import shutil
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('reddit averages').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.1' # make sure we have Spark 2.1+


schema = types.StructType([ # commented-out fields won't be read
    types.StructField('language', types.StringType(), False),
    types.StructField('name', types.StringType(), False),
    types.StructField('requests', types.LongType(), False),
    types.StructField('views', types.LongType(), False),
])

def path_to_hour(path):
    #https://stackoverflow.com/questions/678236/how-to-get-the-filename-without-the-extension-from-a-path-in-python
    return os.path.splitext(path)[0][-15:-4]

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
    # date = {"Date/Time"}
    # numbers = {"Temp (°C)","Dew Point Temp (°C)","Rel Hum (%)","Wind Dir (10s deg)","Wind Spd (km/h)","Visibility (km)","Stn Press (kPa)","Hmdx","Wind Chill"}
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
    print(headers)
    print(schema)
    weather = spark.read.csv(directory, schema=schema)#.withColumn('filename', functions.input_file_name())
    print(weather.schema)
    weather = weather[weather['Weather'] != 'NA']
    weather.show()
    weather_columns = [i for i in weather.schema.names if 'Flag' not in i]
    print(weather_columns)
    weather = weather.select(
        weather_columns
    )
    weather.show()

    # page_data = page_data.filter(page_data['language'] == 'en')
    # page_data = page_data.filter(page_data['name'] != 'Main_Page')
    # page_data = page_data.filter(~page_data['name'].startswith('Special:'))
    #
    # filt_path = functions.UserDefinedFunction(lambda x: path_to_hour(x), types.StringType())
    # page_data = page_data.select(
    #     page_data['language'],
    #     page_data['name'],
    #     page_data['requests'],
    #     page_data['views'],
    #     filt_path(page_data['filename']).alias("hour")
    # )
    # page_data.cache()
    # hour_grouped = page_data.groupBy("hour").agg(functions.max("views"))
    # hour_grouped = hour_grouped.select(
    #     hour_grouped['hour'],
    #     hour_grouped['max(views)'].alias('views')
    # )
    # #hour_grouped.show()
    # #page_data.show()
    #
    # page_data = page_data.join(hour_grouped, ["hour", "views"])
    #
    # page_data = page_data.select(
    #     page_data['hour'],
    #     page_data['name'],
    #     page_data['views']
    # )
    # page_data = page_data.orderBy(functions.asc("hour"), functions.desc("name"))
    #
    # page_data.show()

    weather.write.csv(out_path, mode='overwrite')
    with open('schema', 'w+') as schema_out:
        schema_out.writelines([i + '\n' for i in weather_columns])
    with open('header', 'w+') as header:
        header.writelines(headers)

    shutil.rmtree(directory)
if __name__=='__main__':
    in_directory = sys.argv[1]
    out_path = sys.argv[2]
    main(in_directory, out_path)
