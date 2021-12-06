#!/bin/bash

cp $HOME/mozart/etc/settings.yaml settings_temp.yaml
sed -i 's/\/home\/ops\/verdi/\/export\/home\/hysdsops\/mozart/g' ./settings_temp.yaml

# ingest NEN_L_RRST file
python ~/mozart/ops/opera-pcm/extractor/extract.py --settings ./settings_temp.yaml --workspace ./output $HOME/mozart/ops/opera-pcm/tests/L0A_PGE/test-files/NISAR_S198_ASF_AS4_M00_P00114_R04_C05_G81_2013_123_23_59_59_000000000.vc03
python ~/mozart/ops/hysds/scripts/ingest_dataset.py ./output/ASF_NISAR_VC03_2013_123_23_59_59 $HOME/mozart/etc/datasets.json
