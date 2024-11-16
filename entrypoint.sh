#!/bin/bash
cd lake
dlt project clean
dlt pipeline -l
dlt pipeline linkedin_profiles run
dlt dataset linkedin_profiles info
