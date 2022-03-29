#!/bin/bash

export TEMP_LANDING_ZONE="/user/bdm/tmp"
export GBIF_CREDS="diogo.repas.upc:Dade6cVRmB#5eJ"

cd collectors

# Run in parallel and dettached from this shell (allow closing terminal while running)
nohup sh -c "./GBIF.sh" &> /tmp/GBIF_collect.log &
nohup sh -c "./WorldClim.sh" &> /tmp/WorldClim_collect.log &

cd ..

# Optionally check log file
#tail -f /tmp/GBIF_collect.log
#tail -f /tmp/WorldClim_collect.log