#!/bin/bash

# Temporary fix for the issues with not having a fully-activated virual
# environment in the Tower runtime environment at execution time.
source .venv/bin/activate

export PYTHONPATH=$(pwd)
echo "username $(uname)"

cd lake
dlt project clean
dlt pipeline -l
dlt pipeline linear run
dlt dataset linear info
