import sys
import cv2
import json
import os
import glob

in_directory = sys.argv[1] # should be katkam-scaled
out_directory = sys.argv[2] # should be katkam-rgb-json

def path_to_time(path):
    timestamp = os.path.splitext(path)[0][-14:]
    #2017-05-01 16:00
    return "{}-{}-{} {}:00".format(timestamp[-14:-10], timestamp[-10:-8], timestamp[-8:-6], timestamp[-6:-4] )

def main():
    images = []
    #df = spark.createDataFrame([], schema)
    try:
        os.makedirs(os.path.dirname('{}/'.format('katkam-rgb-json')))
    except Exception as e:
        print(e)

    # Read images from katkam-scaled folder, write to json and then read into spark -> avoids memory issues
    for filename in glob.glob('{}/*.jpg'.format(in_directory)):
        img = cv2.imread(filename).flatten().tolist()
        with open((out_directory + '/{}').format(os.path.splitext(filename)[0][-21:]), 'w') as fp:
            json.dump({'time':path_to_time(filename), 'image': img}, fp)
        #images.append(img_row)
        #img_row = Row(time=path_to_time(filename), image=img)

        #df = df.union(img_row)

    #df = spark.createDataFrame(images)


if __name__=='__main__':
    main()