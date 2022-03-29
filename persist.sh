#!/bin/bash

export TEMP_LANDING_ZONE="/tmp"
export PERSIST_LANDING_ZONE="/data"

# Run in parallel and dettached from this shell (allow closing terminal while running)
nohup sh -c "./persisters/GBIF.sh" &> /tmp/GBIF_persist.log &
nohup sh -c "./persisters/WorldClim.sh" &> /tmp/WorldClim_persist.log &

# TODO: Optionally check log file
# tail -f /tmp/GBIF_persist.log
tail -f /tmp/WorldClim_persist.log