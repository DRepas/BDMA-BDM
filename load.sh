#!/bin/bash

cd loaders

# Run in parallel and dettached from this shell (allow closing terminal while running)
nohup python3 GBIF.py &> /tmp/GBIF_load.log &
nohup python3 WorldClim.py &> /tmp/WorldClim_load.log &

cd ..

# Optionally check log file
#tail -f /tmp/GBIF_load.log
#tail -f /tmp/WorldClim_load.log