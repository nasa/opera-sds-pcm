#!/bin/bash

cp $HOME/mozart/etc/settings.yaml settings_temp.yaml
sed -i 's/\/home\/ops\/verdi/\/export\/home\/hysdsops\/mozart/g' ./settings_temp.yaml

python ~/mozart/ops/opera-pcm/extractor/extract.py --settings ./settings_temp.yaml --workspace ./output $HOME/mozart/ops/opera-pcm/tests/orbits/test-files/NISAR_ANC_J_PR_FOE_20200504T145019_20200603T225942_20200610T225942.xml.gz
python ~/mozart/ops/hysds/scripts/ingest_dataset.py ./output/NISAR_ANC_J_PR_FOE_20200504T145019_20200603T225942_20200610T225942 $HOME/mozart/etc/datasets.json

python ~/mozart/ops/opera-pcm/extractor/extract.py --settings ./settings_temp.yaml --workspace ./output $HOME/mozart/ops/opera-pcm/tests/orbits/test-files/NISAR_ANC_J_PR_MOE_20220104T145019_20220108T050942_20220108T235942.xml.gz
python ~/mozart/ops/hysds/scripts/ingest_dataset.py ./output/NISAR_ANC_J_PR_MOE_20220104T145019_20220108T050942_20220108T235942 $HOME/mozart/etc/datasets.json

python ~/mozart/ops/opera-pcm/extractor/extract.py --settings ./settings_temp.yaml --workspace ./output $HOME/mozart/ops/opera-pcm/tests/orbits/test-files/NISAR_ANC_J_PR_NOE_20200504T145019_20200504T225942_20200504T234442.xml.gz
python ~/mozart/ops/hysds/scripts/ingest_dataset.py ./output/NISAR_ANC_J_PR_NOE_20200504T145019_20200504T225942_20200504T234442 $HOME/mozart/etc/datasets.json

python ~/mozart/ops/opera-pcm/extractor/extract.py --settings ./settings_temp.yaml --workspace ./output $HOME/mozart/ops/opera-pcm/tests/orbits/test-files/NISAR_ANC_J_PR_POE_20220104T145019_20220108T125942_20220108T225942.xml.gz
python ~/mozart/ops/hysds/scripts/ingest_dataset.py ./output/NISAR_ANC_J_PR_POE_20220104T145019_20220108T125942_20220108T225942 $HOME/mozart/etc/datasets.json
