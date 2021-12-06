#!/bin/bash

cp $HOME/mozart/etc/settings.yaml settings_temp.yaml
sed -i 's/\/home\/ops\/verdi/\/export\/home\/hysdsops\/mozart/g' ./settings_temp.yaml

python ~/mozart/ops/opera-pcm/extractor/extract.py --settings ./settings_temp.yaml --workspace ./output $HOME/mozart/ops/opera-pcm/tests/radar_pointing/test-files/NISAR_ANC_J_PR_FRP_20190907145012_20180604145943_20180605004952.xml
python ~/mozart/ops/hysds/scripts/ingest_dataset.py ./output/NISAR_ANC_J_PR_FRP_20190907145012_20180604145943_20180605004952 $HOME/mozart/etc/datasets.json

python ~/mozart/ops/opera-pcm/extractor/extract.py --settings ./settings_temp.yaml --workspace ./output $HOME/mozart/ops/opera-pcm/tests/radar_pointing/test-files/NISAR_ANC_J_PR_NRP_20190903145017_20180605005946_20180605051142.xml
python ~/mozart/ops/hysds/scripts/ingest_dataset.py ./output/NISAR_ANC_J_PR_NRP_20190903145017_20180605005946_20180605051142 $HOME/mozart/etc/datasets.json

python ~/mozart/ops/opera-pcm/extractor/extract.py --settings ./settings_temp.yaml --workspace ./output $HOME/mozart/ops/opera-pcm/tests/radar_pointing/test-files/NISAR_ANC_J_PR_PRP_20190904145529_20180605005959_20180607005823.xml
python ~/mozart/ops/hysds/scripts/ingest_dataset.py ./output/NISAR_ANC_J_PR_PRP_20190904145529_20180605005959_20180607005823 $HOME/mozart/etc/datasets.json

python ~/mozart/ops/opera-pcm/extractor/extract.py --settings ./settings_temp.yaml --workspace ./output $HOME/mozart/ops/opera-pcm/tests/radar_pointing/test-files/NISAR_ANC_L_PR_FRP_20190905145019_20180603125941_20180603225542.xml
python ~/mozart/ops/hysds/scripts/ingest_dataset.py ./output/NISAR_ANC_L_PR_FRP_20190905145019_20180603125941_20180603225542 $HOME/mozart/etc/datasets.json

python ~/mozart/ops/opera-pcm/extractor/extract.py --settings ./settings_temp.yaml --workspace ./output $HOME/mozart/ops/opera-pcm/tests/radar_pointing/test-files/NISAR_ANC_L_PR_NRP_20190901145015_20180602125944_20180602174942.xml
python ~/mozart/ops/hysds/scripts/ingest_dataset.py ./output/NISAR_ANC_L_PR_NRP_20190901145015_20180602125944_20180602174942 $HOME/mozart/etc/datasets.json

python ~/mozart/ops/opera-pcm/extractor/extract.py --settings ./settings_temp.yaml --workspace ./output $HOME/mozart/ops/opera-pcm/tests/radar_pointing/test-files/NISAR_ANC_L_PR_PRP_20190904145019_20180603225947_20180605005942.xml
python ~/mozart/ops/hysds/scripts/ingest_dataset.py ./output/NISAR_ANC_L_PR_PRP_20190904145019_20180603225947_20180605005942 $HOME/mozart/etc/datasets.json

python ~/mozart/ops/opera-pcm/extractor/extract.py --settings ./settings_temp.yaml --workspace ./output $HOME/mozart/ops/opera-pcm/tests/radar_pointing/test-files/NISAR_ANC_S_PR_FRP_20190906145011_20180606135942_20180607004942.xml
python ~/mozart/ops/hysds/scripts/ingest_dataset.py ./output/NISAR_ANC_S_PR_FRP_20190906145011_20180606135942_20180607004942 $HOME/mozart/etc/datasets.json

python ~/mozart/ops/opera-pcm/extractor/extract.py --settings ./settings_temp.yaml --workspace ./output $HOME/mozart/ops/opera-pcm/tests/radar_pointing/test-files/NISAR_ANC_S_PR_NRP_20190902145016_20180607105945_20180607151942.xml
python ~/mozart/ops/hysds/scripts/ingest_dataset.py ./output/NISAR_ANC_S_PR_NRP_20190902145016_20180607105945_20180607151942 $HOME/mozart/etc/datasets.json

python ~/mozart/ops/opera-pcm/extractor/extract.py --settings ./settings_temp.yaml --workspace ./output $HOME/mozart/ops/opera-pcm/tests/radar_pointing/test-files/NISAR_ANC_S_PR_PRP_20190904145219_20180607005948_20180609005955.xml
python ~/mozart/ops/hysds/scripts/ingest_dataset.py ./output/NISAR_ANC_S_PR_PRP_20190904145219_20180607005948_20180609005955 $HOME/mozart/etc/datasets.json
