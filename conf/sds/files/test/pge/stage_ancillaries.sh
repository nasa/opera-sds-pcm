#!/bin/bash

echo "**************** Copying COP files input to ISL ****************"
aws s3 cp ~/mozart/ops/opera-pcm/tests/cop/test-files/COP_e2020-040_c2020-040_v001.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/cop/test-files/COP_e2020-041_c2020-041_v001.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/cop/test-files/COP_e2021-188_c2021-188_v001.xml s3://$1/gds/


echo "**************** Copying TIURDROP files input to ISL ****************"
aws s3 cp ~/mozart/ops/opera-pcm/tests/cop/test-files/TIURDROP_UR_event_fire_2021_04_12T00_00_00_000Z_TIM_nicc_usda_cloudvault_e2023-016_c2021-105_v000.xml.xopsis.xml s3://$1/gds/

echo "**************** Copying ROST files input to ISL ****************"
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-11-0100_orost-2023002-c005-d02-v01.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-1a-0100_orost-2023015-c001-d15-v01.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-1a-0100_orost-2023016-c001-d16-v01.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-1a-0100_orost-2023022-c001-d22-v01.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-1b-0100_orost-2020275-c005-d60-v01.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-1b-0100_orost-2020081-c001-d81-v01.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-0a-0100_orost-2021188-c001-d01-v01.xml s3://$1/gds/

echo "**************** Copying POE to ISL ****************"
aws s3 cp $HOME/mozart/ops/opera-pcm/tests/pge/l0b/NISAR_ANC_J_PR_POE_20180504T145019_20180603T000000_20180605T005942.xml.gz s3://$1/gds/

echo "**************** Copying STUF to ISL ****************"
aws s3 cp ~/mozart/ops/opera-pcm/tests/stuf/test-files/opera_stuf_20190213184428_20211228115850_20220109115850.xml s3://$1/
aws s3 cp ~/mozart/ops/opera-pcm/tests/stuf/test-files/opera_stuf_20201201232340_20200125235959_20200210000001.xml s3://$1/
aws s3 cp ~/mozart/ops/opera-pcm/tests/stuf/test-files/opera_stuf_20201202232340_20200209235959_20200225000001.xml s3://$1/
aws s3 cp ~/mozart/ops/opera-pcm/tests/stuf/test-files/opera_stuf_20060101232340_20061006235959_20061013000001.xml s3://$1/
aws s3 cp ~/mozart/ops/opera-pcm/tests/stuf/test-files/opera_stuf_20080212232340_20080212235959_20080225060000.xml s3://$1/
aws s3 cp ~/mozart/ops/opera-pcm/tests/stuf/test-files/opera_stuf_20080401232340_20080401235959_20080414060000.xml s3://$1/
aws s3 cp ~/mozart/ops/opera-pcm/tests/stuf/test-files/opera_stuf_20201001232340_20201001235959_20201003000001.xml s3://$1/
aws s3 cp ~/mozart/ops/opera-pcm/tests/stuf/test-files/opera_stuf_20210706232340_20210706235959_20210720060000.xml s3://$1/
