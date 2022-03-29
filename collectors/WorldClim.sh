#!/bin/bash

temporary_landing_zone="/tmp"

version="2.1"
resolution="30s"
variables=( "tmin" "tmax" "tavg" "prec" "srad" "wind" "vapr" "bio" "elev" )
for variable in "${variables[@]}"
do
    download_link="https://biogeo.ucdavis.edu/data/worldclim/v${version}/base/wc${version}_${resolution}_${variable}.zip"
    zipfile="$temporary_landing_zone/wc${version}_${resolution}_${variable}.zip"
    echo Downloading $download_link to $zipfile
    curl $download_link -L | ~/BDM_Software/hadoop/bin/hdfs dfs -put -f - $zipfile
done