#!/bin/bash

setup() {
    export PYSPARK_PYTHON=python3
    module load 318
    pip3 install opencv-python numpy

}
clean_weather() {
    hdfs dfs -put yvr-weather yvr-weather

    python3 weather_setup.py yvr-weather cleaned-weather tempdir
    hdfs dfs -put tempdir tempdir
    hdfs dfs -put schema schema
    hdfs dfs -put header header
    spark-submit weather_parse.py yvr-weather cleaned-weather tempdir
    hdfs dfs -rm schema
    hdfs dfs -put schema schema

}

remove_all() {
    hdfs dfs -rm -r -f cleaned-katkam-greyscale cleaned-weather headers katkam-greyscaled-json schema tempdir yvr-weather
}
write_katkam_json_greyscale() {
    python3 write_katkam_json.py katkam-scaled katkam-greyscaled-json 0
}

write_katkam_json_rgb() {
    python3 write_katkam_json.py katkam-scaled katkam-rgb-json 1
}

put_katkam_with_time() {
    hdfs dfs -put katkam-greyscaled-json katkam-greyscaled-json
}

add_time_to_image() {
    spark-submit pair_images_by_time.py katkam-greyscaled-json cleaned-katkam-greyscale cleaned-weather
}

analyze() {
    spark-submit analysis.py cleaned-katkam-greyscale cleaned-weather
}

CLEAN_DFS=0
SETUP=1
CLEAN_WEATHER=1
CLEAN_IMAGES=1
ANALYZE=1
COLOR=0

for i in "$@"
do
case $i in
    --no-color)
    COLOR=0
    shift # passed argument=value
    ;;
    --clean-dfs)
    CLEAN_DFS=1
    shift # passed argument=value
    ;;
    --no-setup)
    SETUP=0
    shift # passed argument=value
    ;;
    --no-clean-images)
    CLEAN_IMAGES=0
    shift # passed argument=value
    ;;
    --no-clean-weather)
    CLEAN_WEATHER=0
    shift # passed argument=value
    ;;
    --no-analyze)
    ANALYZE=0
    shift # passed argument with no value
    ;;
    *)
          # unknown option
    ;;
esac
done
if [ $SETUP = 1 ]; then
    setup
fi
if [ $CLEAN_DFS = 1 ]; then
    remove_all
fi

if [ $CLEAN_WEATHER = 1 ]; then
    clean_weather
fi

if [ $CLEAN_IMAGES = 1 ]; then
    if [ $COLOR = 1 ]; then
        write_katkam_json_rgb
        put_katkam_with_time
        add_time_to_image
    fi

    if [ $COLOR = 0 ]; then
        write_katkam_json_greyscale
        put_katkam_with_time
        add_time_to_image
    fi
fi

if [ $ANALYZE = 1 ]; then
    analyze
fi

