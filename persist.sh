#!/bin/bash

export TEMP_LANDING_ZONE="/user/bdm/tmp"
export PERSIST_LANDING_ZONE="/user/bdm/data"

cd persisters

# Run in parallel and dettached from this shell (allow closing terminal while running)
nohup sh -c "./GBIF.sh" &> /tmp/GBIF_persist.log &
nohup sh -c "./WorldClim.sh" &> /tmp/WorldClim_persist.log &

cd ..

# Optionally check log file
#tail -f /tmp/GBIF_persist.log
#tail -f /tmp/WorldClim_persist.log