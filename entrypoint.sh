#!/bin/bash
export PYTHONPATH=$(pwd)
cd lake
dlt project clean
dlt pipeline -l
dlt pipeline --profile=dev linear run
dlt dataset linear info
ls ./_storage
if [ -d "./_storage" ]; then
  echo "Copying data from ./_storage to $LOCAL_STORAGE_VOLUME"
  mkdir -p $LOCAL_STORAGE_VOLUME
  cp -R ./_storage $LOCAL_STORAGE_VOLUME
fi

