#!/bin/bash

cd collectors

# Run in parallel and dettached from this shell (allow closing terminal while running)
nohup python3 GBIF.py &> /tmp/GBIF_collect.log &
nohup python3 WorldClim.py &> /tmp/WorldClim_collect.log &

cd ..

# Optionally check log file
#tail -f /tmp/GBIF_collect.log
#tail -f /tmp/WorldClim_collect.log