#!/bin/bash

setup() {
    export PYSPARK_PYTHON=python3
    module load 318
    pip3 install opencv-python numpy

}
clean_weather() {
    hdfs dfs -mkdir tempdir
    spark-submit weather_parse.py yvr-weather cleaned-weather
    hdfs dfs -put schema schema
    hdfs dfs -put schema headers

}

write_greyscale_json() {
    python3 write_greyscale_json.py katkam-scaled katkam-greyscaled-json
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



home_space()
{
    # Only the superuser can get this information

    if [ "$(id -u)" = "0" ]; then
        echo "<h2>Home directory space by user</h2>"
        echo "<pre>"
        echo "Bytes Directory"
        du -s /home/* | sort -nr
        echo "</pre>"
    fi

}   # end of home_space

SETUP=1
CLEAN_WEATHER=1
CLEAN_IMAGES=1
ANALYZE=1
for i in "$@"
do
case $i in
    --no-setup)
    SETUP=0
    shift # past argument=value
    ;;
    --no-clean-images)
    CLEAN_IMAGES=0
    shift # past argument=value
    ;;
    --no-clean-weather)
    CLEAN_WEATHER=0
    shift # past argument=value
    ;;
    --no-analyze)
    ANALYZE=0
    shift # past argument with no value
    ;;
    *)
          # unknown option
    ;;
esac
done
if [ $SETUP = 1 ]; then
    setup
fi

if [ $CLEAN_WEATHER = 1 ]; then
    clean_weather
fi

if [ $CLEAN_IMAGES = 1 ]; then
    write_greyscale_json
    put_katkam_with_time
    add_time_to_image
fi

if [ $ANALYZE = 1 ]; then
    analyze
fi

