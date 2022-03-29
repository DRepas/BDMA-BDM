#!/bin/bash

# TODO: Check TEMP_LANDING_ZONE and PERSIST_LANDING_ZONE are set

# TODO: Do this for all resolutions and variables found in temporal landing zone that are finished downloading (find files recursively inside $TEMP_LANDING_ZONE/wc/)
source="wc"
version="2.1"
resolution="30s"
variable="tavg"

timestamp=$(date +"%Y-%m-%d_%H-%M-%S")

zipfile=$TEMP_LANDING_ZONE/$source/${version}/${resolution}_${variable}.zip
outdir=$PERSIST_LANDING_ZONE/$source/$timestamp
localdir=/tmp/$source/$timestamp

# Download from HDFS and extract in local file system
mkdir -p $localdir
~/BDM_Software/hadoop/bin/hdfs dfs -cat $zipfile | bsdtar -xvf- -C $localdir

# Upload to HDFS
~/BDM_Software/hadoop/bin/hdfs dfs -mkdir $outdir
~/BDM_Software/hadoop/bin/hdfs dfs -put $localdir $outdir

# Remove redundant zip files (local and hdfs)
rm -r $localdir
~/BDM_Software/hadoop/bin/hdfs dfs -rm $zipfile