import sys
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.column import _to_java_column, _to_seq, Column
from pyspark import SparkContext
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.types import StructType, StructField, StringType, LongType

def as_vector(col):
    sc = SparkContext.getOrCreate()
    f = sc._jvm.com.example.spark.udfs.udfs.as_vector()
    return Column(f.apply(_to_seq(sc, [col], _to_java_column)))

schema = StructType([
    StructField('Date/Time',StringType(),True),
    StructField("image",VectorUDT(),False)
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

cleaned_katkam = sys.argv[1] # 'cleaned-katkam'
cleaned_weather = sys.argv[2] # 'cleaned-weather'

def rain_gone(vs):
    return 0 if 'Rain' in vs else 1


def main():
    df = spark.read.json(katkam_in_directory)
    schema_file = open('schema')
    schema_lines = [i.strip() for i in schema_file.readlines()]
    schema = types.StructType([types.StructField(i, types.StringType(), False) for i in schema_lines])
    schema_file.close()
    weather = spark.read.csv(weather_in_directory, schema=schema)#.withColumn('filename', functions.input_file_name())

    df = df.join(weather, 'Date/Time')

    # https://stackoverflow.com/questions/39025707/how-to-convert-arraytype-to-densevector-in-pyspark-dataframe
    to_vec = functions.UserDefinedFunction(lambda vs: Vectors.dense(vs), VectorUDT())
    get_rid_of_rain = functions.UserDefinedFunction(lambda vs: rain_gone(vs), LongType())

    df = df.select(get_rid_of_rain(df['Weather']).alias('label'), to_vec(df['image']).alias('features'))
    df.show()
    print(df.schema)
    splits = df.randomSplit([0.6, 0.4], 1234)
    train = splits[0]
    test = splits[1]
    nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
    model = nb.fit(train)
    predictions = model.transform(test)
    predictions.show()

    # compute accuracy on the test set
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                                  metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print("Test set accuracy = " + str(accuracy))

if __name__=='__main__':
    main()