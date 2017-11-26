import sys
# from pyspark.sql import SparkSession, functions, types
import numpy as np
import cv2
from skimage.color import lab2rgb, rgb2lab
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import FunctionTransformer
from sklearn.pipeline import make_pipeline

'''
spark = SparkSession.builder.appName('weather classification').getOrCreate()

assert sys.version_info >= (3, 4)  # make sure we have Python 3.4+
assert spark.version >= '2.1'  # make sure we have Spark 2.1+

# Placeholder schema based on exercise 11
schema = types.StructType([
    types.StructField('score', types.LongType(), False),
    #types.StructField('score_hidden', types.BooleanType(), False),
    types.StructField('subreddit', types.StringType(), False),
])
'''

def main():

    # Read a single image from katkam-scaled folder, use spark later
    image_path = 'katkam-scaled/katkam-20160605060000.jpg'

    # img is a 3D numpy array, that is, a 2D numpy array of RGB lists
    img = cv2.imread(image_path)

    cv2.imshow('image', img)
    print(img)

    # TODO: Figure out what to do with img: Flatten?

    # Boilerplate for ML stuff

    # train = pd.read_json('train.json')
    # X = pd.DataFrame(i[0] + i[1] for i in list(zip(list(train['band_1'].values), list(train['band_2'].values))))
    # X = pd.DataFrame(list(train['band_1'].values))
    # pd.DataFrame([np.append(*row) for row in ]
    # X = train[['band_1', 'band_2', 'inc_angle']]
    # y = train['is_iceberg']
    # X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    # model = KNeighborsClassifier()
    # model.fit(X_train, y_train)
    # print(model.score(X_test, y_test))


if __name__=='__main__':
    main()