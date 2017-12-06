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
    spark-submit --num-executors=100 --conf spark.dynamicAllocation.enabled=false --conf spark.yarn.executor.memoryOverhead=10G --conf spark.executor.memory=100G weather_parse.py yvr-weather cleaned-weather tempdir
    hdfs dfs -rm schema
    hdfs dfs -put schema schema

}

clean_tides() {
    hdfs dfs -put tide-folder
    spark-submit tide_data_clean.py tide-folder tide-cleaned
    hdfs dfs -put tide-cleaned
}

remove_all() {
    hdfs dfs -rm -r -f cleaned-katkam-greyscale katkam-rgb-json cleaned-weather headers katkam-greyscaled-json schema tempdir yvr-weather
}

write_katkam_json_greyscale() {
    python3 write_katkam_json.py katkam-scaled katkam-greyscaled-json 0
    hdfs dfs -put katkam-greyscaled-json katkam-greyscaled-json
}

write_katkam_json_rgb() {
    python3 write_katkam_json.py katkam-scaled katkam-rgb-json 1
    hdfs dfs -put katkam-rgb-json katkam-rgb-json
}

add_time_to_image_greyscale() {
    spark-submit  --conf spark.dynamicAllocation.enabled=false --conf spark.yarn.executor.memoryOverhead=10G --conf spark.executor.memory=100G --num-executors=100 pair_images_by_time.py katkam-greyscaled-json cleaned-weather cleaned-katkam-greyscale
}

add_time_to_image_rgb() {
    spark-submit  --conf spark.dynamicAllocation.enabled=false --conf spark.yarn.executor.memoryOverhead=10G --conf spark.executor.memory=100G --num-executors=100 pair_images_by_time.py katkam-rgb-json cleaned-weather cleaned-katkam-rgb
}

analyze_tides() {
    spark-submit --conf spark.dynamicAllocation.enabled=false --conf spark.yarn.executor.memoryOverhead=10G --conf spark.executor.memory=100G --num-executors=100 tide_data_analysis.py tide-cleaned cleaned-katkam-rgb final-results-tides
}

analyze_greyscale() {
    spark-submit  --conf spark.dynamicAllocation.enabled=false --conf spark.yarn.executor.memoryOverhead=10G --conf spark.executor.memory=100G --num-executors=100 analysis.py cleaned-katkam-greyscale cleaned-weather final-results
}

analyze_rgb() {
    spark-submit --conf spark.dynamicAllocation.enabled=false --conf spark.yarn.executor.memoryOverhead=10G --conf spark.executor.memory=100G --num-executors=100 analysis.py cleaned-katkam-rgb cleaned-weather final-results
}

CLEAN_DFS=0
SETUP=1
CLEAN_WEATHER=1
CLEAN_IMAGES=1
ANALYZE=1
COLOR=1

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
    --analyze-tides)
    ANALYZE=2
    CLEAN_WEATHER=0
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
        add_time_to_image_rgb
    fi

    if [ $COLOR = 0 ]; then
        write_katkam_json_greyscale
        add_time_to_image_greyscale
    fi
fi

if [ $ANALYZE = 1 ]; then
    if [ $COLOR = 1 ]; then
        analyze_rgb
    fi

    if [ $COLOR = 0 ]; then
        analyze_greyscale
    fi
fi

if [ $ANALYZE = 2 ]; then
    analyze_tides
fi

