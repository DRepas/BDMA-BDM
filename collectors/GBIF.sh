#!/bin/bash

temporary_landing_zone="/tmp"

version="v1"
download_key="0196579-210914110416597" #$(curl -s --user "diogo.repas.upc:Dade6cVRmB#5eJ" --header "Content-Type: application/json" --data @GBIF_query.json https://api.gbif.org/$version/occurrence/download/request)
echo Download request key is $download_key.

while :
do 
    request=$(curl -s https://api.gbif.org/$version/occurrence/download/$download_key)
    status=$(echo $request | jq .status | tr -d '"')
    echo Download request status is $status.
    if [ $status == "SUCCEEDED" ]
    then
        break
    fi
    sleep 5
done
download_link=$(echo $request | jq .downloadLink | tr -d '"')
zipfile="$temporary_landing_zone/gbif$version-$download_key.zip"
echo Downloading $download_link to $zipfile
curl $download_link -L | ~/BDM_Software/hadoop/bin/hdfs dfs -put -f - $zipfile