#!/bin/bash

# Run in dettached tmux sessions (in parallel), allows exiting ssh
tmux new -d -s GBIF 'cd collectors && python3 GBIF.py && cd ..\loaders && python3 GBIF.py && cd ..'
tmux new -d -s WorldClim 'cd collectors && python3 WorldClim.py && cd ..\loaders && python3 WorldClim.py && cd ..'
tmux new -d -s NEO 'cd collectors && python3 NEO.py && cd ..\loaders && python3 NEO.py && cd ..'