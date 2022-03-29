#!/bin/bash

export TEMP_LANDING_ZONE="/tmp"
export GBIF_CREDS="diogo.repas.upc:Dade6cVRmB#5eJ"

# Run in parallel and dettached from this shell (allow closing terminal while running)
nohup sh -c "./collectors/GBIF.sh" &> /tmp/GBIF_collect.log &
nohup sh -c "./collectors/WorldClim.sh" &> /tmp/WorldClim_collect.log &

# TODO: Optionally check log file
# tail -f /tmp/GBIF_collect.log
tail -f /tmp/WorldClim_collect.log