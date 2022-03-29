#!/bin/bash

# Configuration parameters
source="wc"
version="2.1"

# Validate environment variables
if [[ -z "${TEMP_LANDING_ZONE}" ]]
then
    echo "Environment variable TEMP_LANDING_ZONE not set, should be a path in HDFS"
    exit 1
fi

if [[ -z "${PERSIST_LANDING_ZONE}" ]]
then
    echo "Environment variable PERSIST_LANDING_ZONE not set, should be a path in HDFS"
    exit 1
fi

# Find all downloads in temporal landing zone
zipfile_dir=$TEMP_LANDING_ZONE/$source/$version
zipfiles=$(~/BDM_Software/hadoop/bin/hdfs dfs -ls -R $zipfile_dir | awk '{print $NF}' | grep .zip$ | tr '\n' ' ')
for zipfile in $zipfiles
do
    # Extract resolution and variable from file
    resolution=$(basename $zipfile .zip | cut -d _ -f 1)
    variable=$(basename $zipfile .zip | cut -d _ -f 2)

    # Create timestamped local and persistent (HDFS) directories
    timestamp=$(date +"%Y-%m-%d_%H-%M-%S")
    outdir=$PERSIST_LANDING_ZONE/$source/$timestamp
    localdir=/tmp/$source/$timestamp

    # Download from HDFS and extract in local file system
    mkdir -p $localdir
    ~/BDM_Software/hadoop/bin/hdfs dfs -cat $zipfile | bsdtar -xvf- -C $localdir

    # Upload contents to HDFS
    ~/BDM_Software/hadoop/bin/hdfs dfs -mkdir $outdir
    ~/BDM_Software/hadoop/bin/hdfs dfs -put $localdir $outdir

    # Remove redundant zip files (local and hdfs)
    rm -r $localdir
    ~/BDM_Software/hadoop/bin/hdfs dfs -rm $zipfile
done