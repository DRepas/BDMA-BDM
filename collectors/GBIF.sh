#!/bin/bash

# Configuration parameters
version="1"

# Validate environment variables
if [[ -z "${TEMP_LANDING_ZONE}" ]]
then
    echo "Environment variable TEMP_LANDING_ZONE not set, should be a path in HDFS"
    exit 1
fi

if [[ -z "${GBIF_CREDS}" ]]
then
    echo "Environment variable GBIF_CREDS not set, should be a string of <gbif_username>:<gbif_password>"
    exit 1
fi

# Perform download request
download_key=$(curl -s --user "$GBIF_CREDS" --header "Content-Type: application/json" --data @GBIF_query.json https://api.gbif.org/v$version/occurrence/download/request)
echo "Download request key is $download_key."

# Actively wait for download to be done (ping every 5s)
while :
do 
    request=$(curl -s https://api.gbif.org/v$version/occurrence/download/$download_key)
    status=$(echo $request | jq .status | tr -d '"')
    echo -ne "Download request status is $status."\\r

    if [ $status == "SUCCEEDED" ]
    then
        break
    fi
    
    sleep 5
done

# Extract download link and storage destination
download_link=$(echo $request | jq .downloadLink | tr -d '"')
zipdir=$TEMP_LANDING_ZONE/gbif/$version
zipfile=$zipdir/$download_key.zip
echo "Downloading $download_link to $zipfile"

# Download to HDFS
~/BDM_Software/hadoop/bin/hdfs dfs -mkdir -p $zipdir
curl $download_link -L | ~/BDM_Software/hadoop/bin/hdfs dfs -put -f - $zipfile