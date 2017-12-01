#!/bin/bash

# sysinfo_page - A script to produce a system information HTML file

##### Constants
USER=rmahey
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
    hdfs dfs -put katkamkatkam-greyscaled-json katkam-greyscaled-json
}

add_time_to_image() {
    spark-submit pair_images_by_time.py katkam-json cleaned-katkam-greyscale cleaned-weather
}

analyze() {
    spark-submit analysis.py cleaned-katkam-greyscale cleaned-weather
}

hdfs dfs -put cleaned-katkam $PWD/cleaned-katkam

TITLE="System Information for $HOSTNAME"
RIGHT_NOW=$(date +"%x %r %Z")
TIME_STAMP="Updated on $RIGHT_NOW by $USER"

##### Functions

system_info()
{
    echo "<h2>System release info</h2>"
    echo "<p>Function not yet implemented</p>"

}   # end of system_info


show_uptime()
{
    echo "<h2>System uptime</h2>"
    echo "<pre>"
    uptime
    echo "</pre>"

}   # end of show_uptime


drive_space()
{
    echo "<h2>Filesystem space</h2>"
    echo "<pre>"
    df
    echo "</pre>"

}   # end of drive_space


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



##### Main

cat <<- _EOF_
  <html>
  <head>
      <title>$TITLE</title>
  </head>
  <body>
      <h1>$TITLE</h1>
      <p>$TIME_STAMP</p>
      $(system_info)
      $(show_uptime)
      $(drive_space)
      $(home_space)
  </body>
  </html>
_EOF_

