# pip3 install nose pillow keras h5py py4j
import sys
from pyspark.ml.classification import NaiveBayes, LinearSVC, MultilayerPerceptronClassifier, LogisticRegression, OneVsRest, RandomForestClassifier, GBTClassifier, LogisticRegressionSummary
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.column import _to_java_column, _to_seq, Column
from pyspark import SparkContext
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
#dataframe for testing the classification model
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from sparkdl import DeepImageFeaturizer

spark = SparkSession.builder.appName('Weather Image Classifier - Data Analysis').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+

from sparkdl import readImages

img_dir = "katkam-scaled"

#Read images and Create training & test DataFrames for transfer learning
jobs_df = readImages(img_dir)
jobs_df.show()
df = DeepImageFeaturizer(inputCol="image", outputCol="features", modelName="InceptionV3").transform(jobs_df)
jobs_df.show()
i = 2
jobs_train, jobs_test = jobs_df.randomSplit([0.6, 0.4])



df = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(df).transform(df)

lr = LogisticRegression(maxIter=20, regParam=0.05, elasticNetParam=0.3, labelCol="label")
p = Pipeline(stages=[featurizer, lr])
p_model = p.fit(train_df)
predictions = p_model.transform(test_df)

predictions.select("filePath", "prediction").show(truncate=False)

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
df = p_model.transform(test_df)
df.show()

predictionAndLabels = df.select("prediction", "label")
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
print("Training set accuracy = " + str(evaluator.evaluate(predictionAndLabels)))
