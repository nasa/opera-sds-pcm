#!/bin/bash

cp $HOME/mozart/etc/settings.yaml settings_temp.yaml
sed -i 's/\/home\/ops\/verdi/\/export\/home\/hysdsops\/mozart/g' ./settings_temp.yaml

python ~/mozart/ops/opera-pcm/extractor/extract.py --settings ./settings_temp.yaml --workspace ./output $HOME/mozart/ops/opera-pcm/tests/cop/test-files/COP_e2019-038_c2019-039_v001.xml
python ~/mozart/ops/hysds/scripts/ingest_dataset.py ./output/COP_e2019-038_c2019-039_v001 $HOME/mozart/etc/datasets.json
