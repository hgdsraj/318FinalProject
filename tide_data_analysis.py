import sys
from pyspark.ml.classification import  LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import SparkSession, functions, types

tide_in_directory = sys.argv[1] # should be either cleaned-katkam-grayscale or cleaned-katkam-rgb
katkam_in_directory = sys.argv[2] # should be cleaned-weather
# out_directory = sys.argv[3] # will decide later what output will be, will probably be predictions

spark = SparkSession.builder.appName('Weather Image Classifier - Data Analysis - Tides').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+

def main():
    df = spark.read.json(katkam_in_directory)
    schema = types.StructType([types.StructField('Date/Time', types.StringType(), False),
                               types.StructField('label', types.LongType(), False)])

    tides = spark.read.json(tide_in_directory, schema=schema)
    tides.show()
    df = df.join(tides, 'Date/Time')
    df.show()
    # https://stackoverflow.com/questions/39025707/how-to-convert-arraytype-to-densevector-in-pyspark-dataframe
    to_vec = functions.UserDefinedFunction(lambda vs: Vectors.dense(vs), VectorUDT())
    #df.show()
    df = df.select(df['label'], to_vec(df['image']).alias('features'))

    df.show()

    # Do machine learning
    splits = df.randomSplit([0.6, 0.4], 1234)
    train = splits[0]
    test = splits[1]

    # Logistic Regression Model
    lr = LogisticRegression()
    layers = [147462, 5, 4, 10]

    models = [lr]
    model = [i.fit(train) for i in models]
    predictions = [i.transform(test) for i in model]
    [i.show() for i in predictions]

    # compute accuracy on the test set
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                                  metricName="accuracy")
    accuracy = [evaluator.evaluate(i) for i in predictions]
    for g in accuracy:
        for i in range(20):
            print()
        print("Test set accuracy = " + str(g))



if __name__=='__main__':
    main()
