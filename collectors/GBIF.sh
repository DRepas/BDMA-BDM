#!/bin/bash

# TODO: Check TEMP_LANDING_ZONE and GBIF_CREDS are set

version="1"
download_key=$(curl -s --user "$GBIF_CREDS" --header "Content-Type: application/json" --data @GBIF_query.json https://api.gbif.org/$version/occurrence/download/request)
echo Download request key is $download_key.

while :
do 
    request=$(curl -s https://api.gbif.org/v$version/occurrence/download/$download_key)
    status=$(echo $request | jq .status | tr -d '"')
    echo Download request status is $status.
    if [ $status == "SUCCEEDED" ]
    then
        break
    fi
    sleep 5
done
download_link=$(echo $request | jq .downloadLink | tr -d '"')
zipfile="$TEMP_LANDING_ZONE/gbif/$version/$download_key.zip"
echo Downloading $download_link to $zipfile
curl $download_link -L | ~/BDM_Software/hadoop/bin/hdfs dfs -put -f - $zipfile