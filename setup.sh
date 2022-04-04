#!/bin/bash

# Install dependencies (necessary pip packages)
# TODO: Setup hdfs
# TODO: Setup credentials for harvest
# TODO: Setup environmental variables
export HADOOP_HOME="/home/bdm/BDM_Software/hadoop"
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=$HADOOP_HOME/lib/native"