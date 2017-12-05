import sys
from pyspark.ml.classification import NaiveBayes, LinearSVC, RandomForestClassifier, LogisticRegression, OneVsRest
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.column import _to_java_column, _to_seq, Column
from pyspark import SparkContext
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import SparkSession, functions, types
from pyspark.ml.feature import PCA
from pyspark.sql.types import ArrayType, DoubleType
import matplotlib.pyplot as plt


schema = types.StructType([
    types.StructField('Date/Time', types.StringType(),True),
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

def rain_gone(vs):
    label = 0
    if 'Clear' in vs:
        label = 1
    elif 'Cloudy' in vs:
        label = 2
    elif 'Fog' in vs:
        label = 3
    elif 'Drizzle' in vs:
        label = 4
    elif 'Moderate Rain' in vs:
        label = 5
    elif 'Rain Showers' in vs:
        label = 7
    elif 'Rain' in vs:
        label = 6
    elif 'Snow Showers' in vs:
        label = 9
    elif 'Snow' in vs:
        label = 8
    return label

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
    # https://stackoverflow.com/questions/39025707/how-to-convert-arraytype-to-densevector-in-pyspark-dataframe
    to_vec = functions.UserDefinedFunction(lambda vs: Vectors.dense(vs), VectorUDT())
    get_rid_of_rain = functions.UserDefinedFunction(lambda vs: rain_gone(vs), types.LongType())
    def join_other_columns(x, *args):
        arr = []
        for i in args:
            if i is not None:
                arr.append(float(i))
            else:
                arr.append(float(0))
        return x + arr
    #df.show()

    with_other_columns = functions.UserDefinedFunction(lambda x, *args: join_other_columns(x, *args), ArrayType(DoubleType()))
    #df.show()
    df = df.select(get_rid_of_rain(df['Weather']).alias('label'), to_vec(with_other_columns(df['image'], df['Rel Hum (%)'],
                                                       df['Temp (°C)'], df['Wind Dir (10s deg)'],
                                                       df['Wind Spd (km/h)'], df['Visibility (km)'],
                                                       df['Dew Point Temp (°C)']).alias('image')).alias('features')
                   )


    # TODO: Do KMeans clustering and data visualization
    # kmeans = KMeans(k=8, seed=1)
    # kmeans.fit(df)
    # TODO: try randomforest
    # RandomForestClassifier(numTrees=20)

    # Principal Component Analysis
    # pca = PCA(k=5)
    # model = pca.fit(df)
    # result = model.transform(df).select("pcaFeatures")
    # result.show(truncate=False); return


    # Do machine learning
    splits = df.randomSplit([0.6, 0.4], 1234)
    train = splits[0]
    test = splits[1]

    # Naive Bayes Model
    #nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

    # Logistic Regression Model
    lr = LogisticRegression()

    model = lr.fit(train)
    predictions = model.transform(test)
    predictions.show()

    # compute accuracy on the test set
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                                  metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    for i in range(20):
        print()
    print("Test set accuracy = " + str(accuracy))



if __name__=='__main__':
    main()
