#!/bin/bash

cd lake
dlt project clean
dlt pipeline -l
if [ "$RESOURCE" = all ]; then
    export RESOURCE_CMD=""
else
    export RESOURCE_CMD=" --resource=$RESOURCE"
fi

if [ "$TOWER_ENVIRONMENT" = local ]; then
    dlt pipeline --profile=dev linear run$RESOURCE_CMD
else
    dlt pipeline --profile=prod linear run$RESOURCE_CMD
fi
dlt dataset linear info

# expand the ~ to the home directory when it wasn't passed in properly.
LOCAL_STORAGE_VOLUME="${LOCAL_STORAGE_VOLUME/#\~/$HOME}"

if [ -d "./_storage" ]; then
  echo "Copying data from ./_storage to $LOCAL_STORAGE_VOLUME"
  mkdir -p $LOCAL_STORAGE_VOLUME
  cp -R ./_storage/* $LOCAL_STORAGE_VOLUME
fi

