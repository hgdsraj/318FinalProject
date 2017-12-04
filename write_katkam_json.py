import sys
import cv2
import json
import os
import glob

katkam_in_directory = sys.argv[1] # should be katkam-scaled
out_directory = sys.argv[2] # should be katkam-<rgb/greyscaled>-json
rgb_flag = sys.argv[3] #  should be a 1 for rgb or 0 for greyscale

def path_to_time(path):
    timestamp = os.path.splitext(path)[0][-14:]
    #2017-05-01 16:00
    return "{}-{}-{} {}:00".format(timestamp[-14:-10], timestamp[-10:-8], timestamp[-8:-6], timestamp[-6:-4])

def main():
    try:
        os.makedirs(os.path.dirname('{}/'.format(out_directory)))
    except Exception as e:
        print(e)

    # Read images from katkam-scaled folder, write to json and then read into spark -> avoids memory issues
    in_folder = glob.glob('{}/*.jpg'.format(katkam_in_directory))
    count = len(in_folder)
    for filename in in_folder:
        print(count)
        img = cv2.imread(filename, int(rgb_flag)).flatten().tolist()
        with open('{}/{}'.format(out_directory, os.path.splitext(filename)[0][-21:]), 'w') as fp:
            json.dump({'time':path_to_time(filename), 'image': img}, fp)
        count -= 1

if __name__=='__main__':
    main()