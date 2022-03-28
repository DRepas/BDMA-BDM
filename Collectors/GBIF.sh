#!/bin/bash

download_key="0190379-210914110416597" #$(curl -Ss --include --user "diogo.repas.upc:Dade6cVRmB#5eJ" --header "Content-Type: application/json" --data @download_query.json https://api.gbif.org/v1/occurrence/download/request)
while :
do 
    request=$(curl -Ss https://api.gbif.org/v1/occurrence/download/$download_key)
    status=$(echo $request | jq .status | tr -d '"')
    echo Download request status is $status.
    if [ $status == "SUCCEEDED" ]
    then
        break
    fi
    sleep 5
done
download_link=$(echo $request | jq .downloadLink | tr -d '"')
zipfile="gbif-$download_key.zip"
echo Downloading $download_link to $zipfile
curl $download_link | ~/BDM_Software/hadoop/bin/hdfs hdfs dfs -put -f - $zipfile