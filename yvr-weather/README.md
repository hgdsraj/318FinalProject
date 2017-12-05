# Spark image deep learning CMPT 318 Project
----------------------------------------------------------

## Prerequisites:
 - Spark installed (2.2+)
 - Environment variables set: HADOOP_PATH, HADOOP_HOME, SPARK_PATH, SPARK_PATH, PYSPARK_DRIVER_PYTHON, PYSPARK_PYTHON, PYSPARK_WORKER_PYTHON, SPARK_LOCAL_IP
 - Python 3.4+ installed
 - Hadoop + HDFS installed
 - Weather data in yvr-weather
 - Image data in katkam-scaled with filenames of format katkam-YYYYMMDDHH0000.jpg

----------------------------------------------------------
# How to run:
### To run, simply run `./run.sh`


#### There are various arguments you can pass:
`--no-color`
`--clean-dfs`
`--no-setup`
`--no-clean-images`
`--no-clean-weather`
`--no-analyze`

The commands are not mutually exclusive, everything with "--no-" prepended will be run by default, other commands will be run on top of the other functions.


Explanations:

`--no-color`
    - Run the analysis in Greyscale
`--clean-dfs`
    - Delete the files we created on HDFS (but still run everything else as explained above)
`--no-setup`
    - Do not load 318 module or install required packages (pip)
`--no-clean-images`
    - Do not clean the images
`--no-clean-weather`
    - Do not clean the weather data
`--no-analyze`
    - Do not run the analysis

Example:
    To do analysis only on RGB
        * ./run.sh --no-setup --no-clean-images --no-clean-weather

        