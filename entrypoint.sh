#!/bin/bash

# Temporary fix for the issues with not having a fully-activated virual
# environment in the Tower runtime environment at execution time.
source .venv/bin/activate

cd lake
dlt project clean
dlt pipeline -l
dlt pipeline linkedin_profiles run
dlt dataset linkedin_profiles info
