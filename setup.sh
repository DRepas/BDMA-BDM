#!/bin/bash

# NOTE: All configuration can be found in config/gonfig.json

# TODO: Install dependencies (necessary pip packages)

# Setup HDFS environmental variables
export HADOOP_HOME="/home/bdm/BDM_Software/hadoop"
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=$HADOOP_HOME/lib/native"