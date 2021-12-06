#!/bin/bash

cp $HOME/mozart/etc/settings.yaml settings_temp.yaml
sed -i 's/\/home\/ops\/verdi/\/export\/home\/hysdsops\/mozart/g' ./settings_temp.yaml

python ~/mozart/ops/opera-pcm/extractor/extract.py --settings ./settings_temp.yaml --workspace ./output $HOME/mozart/ops/opera-pcm/tests/time_corr/test-files/NISAR_198900_SCLKSCET_LRCLK.00004
python ~/mozart/ops/hysds/scripts/ingest_dataset.py ./output/NISAR_198900_SCLKSCET_LRCLK.00004 $HOME/mozart/etc/datasets.json

