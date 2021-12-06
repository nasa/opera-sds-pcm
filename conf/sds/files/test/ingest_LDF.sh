#!/bin/bash

cp $HOME/mozart/etc/settings.yaml settings_temp.yaml
sed -i 's/\/home\/ops\/verdi/\/export\/home\/hysdsops\/mozart/g' ./settings_temp.yaml

# ingest LDF file
for file in $HOME/mozart/ops/opera-pcm/tests/ldf/*
do
  echo "$file"
  filename=$(basename -- "$file")
  dataset="${filename%.*}"
  echo "$dataset"
  python ~/mozart/ops/opera-pcm/extractor/extract.py --settings ./settings_temp.yaml --workspace ./output $file
  python ~/mozart/ops/hysds/scripts/ingest_dataset.py ./output/$dataset $HOME/mozart/etc/datasets.json
done
