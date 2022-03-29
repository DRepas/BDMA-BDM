#!/bin/bash

# TODO: Check TEMP_LANDING_ZONE is set

version="2.1"
resolution="30s"
variables=( "tmin" "tmax" "tavg" "prec" "srad" "wind" "vapr" "bio" "elev" )
for variable in "${variables[@]}"
do
    download_link="https://biogeo.ucdavis.edu/data/worldclim/v${version}/base/wc${version}_${resolution}_${variable}.zip"
    outdir=$TEMP_LANDING_ZONE/wc/${version}
    zipfile=$outdir/${resolution}_${variable}.zip
    echo Downloading $download_link to $zipfile

    ~/BDM_Software/hadoop/bin/hdfs dfs -mkdir $outdir
    curl $download_link -L | ~/BDM_Software/hadoop/bin/hdfs dfs -put -f - $zipfile
done