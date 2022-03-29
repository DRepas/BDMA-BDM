#!/bin/bash

# Configuration parameters
version="2.1"
resolutions=( "10m" "5m" "2.5m" "30s" )
variables=( "tmin" "tmax" "tavg" "prec" "srad" "wind" "vapr" "bio" "elev" )

# Validate environment variables
if [[ -z "${TEMP_LANDING_ZONE}" ]]
then
    echo "Environment variable TEMP_LANDING_ZONE not set, should be a path in HDFS"
    exit 1
fi

for resolution in "${resolutions[@]}"
do
    for variable in "${variables[@]}"
    do
        # Download link and storage destination
        download_link="https://biogeo.ucdavis.edu/data/worldclim/v${version}/base/wc${version}_${resolution}_${variable}.zip"
        outdir=$TEMP_LANDING_ZONE/wc/${version}
        zipfile=$outdir/${resolution}_${variable}.zip
        echo Downloading $download_link to $zipfile

        # Download to HDFS
        ~/BDM_Software/hadoop/bin/hdfs dfs -mkdir -p $outdir
        curl $download_link -L | ~/BDM_Software/hadoop/bin/hdfs dfs -put -f - $zipfile
    done
done