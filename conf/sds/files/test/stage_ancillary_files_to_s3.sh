#!/bin/bash

aws s3 cp ~/mozart/ops/opera-pcm/tests/cop/test-files/COP_e2019-038_c2019-039_v001.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/cop/test-files/COP_e2020-041_c2020-041_v001.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/cop/test-files/COP_e2020-041_c2020-041_v002.xml s3://$1/gds/

aws s3 cp ~/mozart/ops/opera-pcm/tests/cop/test-files/TIURDROP_UR_event_fire_2021_04_12T00_00_00_000Z_TIM_nicc_usda_cloudvault_e2023-016_c2021-105_v000.xml.xopsis.xml s3://$1/gds/

#aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-0a-0100_orost-2023001-c001-d01-v01.xml s3://$1/gds/
#aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-0b-0100_orost-2023002-c001-d02-v01.xml s3://$1/gds/
#aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-0c-0100_orost-2023001-c002-d01-v01.xml s3://$1/gds/
#aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-0d-0100_orost-2023002-c002-d02-v01.xml s3://$1/gds/
#aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-0e-0100_orost-2023003-c001-d03-v01.xml s3://$1/gds/
#aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-0f-0100_orost-2023004-c001-d04-v01.xml s3://$1/gds/
#aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-01-0100_orost-2023003-c002-d03-v01.xml s3://$1/gds/
#aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-02-0100_orost-2023004-c002-d04-v01.xml s3://$1/gds/
#aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-03-0100_orost-2023001-c003-d01-v01.xml s3://$1/gds/
#aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-04-0100_orost-2023002-c003-d02-v01.xml s3://$1/gds/
#aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-05-0100_orost-2023003-c003-d03-v01.xml s3://$1/gds/
#aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-06-0100_orost-2023004-c003-d04-v01.xml s3://$1/gds/
#aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-07-0100_orost-2023001-c004-d01-v01.xml s3://$1/gds/
#aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-08-0100_orost-2023002-c004-d02-v01.xml s3://$1/gds/
#aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-09-0100_orost-2023003-c004-d03-v01.xml s3://$1/gds/
#aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-10-0100_orost-2023004-c004-d04-v01.xml s3://$1/gds/
#aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-11-0100_orost-2023001-c005-d01-v01.xml s3://$1/gds/
#aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-13-0100_orost-2023003-c005-d03-v01.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-1a-0100_orost-2023015-c001-d15-v01.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-1a-0100_orost-2023016-c001-d16-v01.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-1a-0100_orost-2023022-c001-d22-v01.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/orost/id_00-1b-0100_orost-2022008-c001-d08-v01.xml s3://$1/gds/

aws s3 cp ~/mozart/ops/opera-pcm/tests/orbits/test-files/NISAR_ANC_J_PR_FOE_20200504T145019_20200603T225942_20200610T225942.xml.gz s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/orbits/test-files/NISAR_ANC_J_PR_MOE_20220104T145019_20220108T050942_20220108T235942.xml.gz s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/orbits/test-files/NISAR_ANC_J_PR_NOE_20200504T145019_20200504T225942_20200504T234442.xml.gz s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/orbits/test-files/NISAR_ANC_J_PR_POE_20220104T145019_20220108T005942_20220108T225942.xml.gz s3://$1/gds/

aws s3 cp ~/mozart/ops/opera-pcm/tests/stuf/test-files/opera_stuf_20220101232340_20220107235959_20220110000001.xml s3://$1/gds/

aws s3 cp ~/mozart/ops/opera-pcm/tests/radar_pointing/test-files/NISAR_ANC_J_PR_FRP_20190907145012_20180604145943_20180605004952.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/radar_pointing/test-files/NISAR_ANC_J_PR_NRP_20190903145017_20180605005946_20180605051142.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/radar_pointing/test-files/NISAR_ANC_J_PR_PRP_20190904145529_20180605005959_20180607005823.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/radar_pointing/test-files/NISAR_ANC_L_PR_FRP_20190905145019_20180603125941_20180603225542.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/radar_pointing/test-files/NISAR_ANC_L_PR_NRP_20190901145015_20180602125944_20180602174942.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/radar_pointing/test-files/NISAR_ANC_L_PR_PRP_20190904145019_20180603225947_20180605005942.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/radar_pointing/test-files/NISAR_ANC_S_PR_FRP_20190906145011_20180606135942_20180607004942.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/radar_pointing/test-files/NISAR_ANC_S_PR_NRP_20190902145016_20180607105945_20180607151942.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/radar_pointing/test-files/NISAR_ANC_S_PR_PRP_20190904145219_20180607005948_20180609005955.xml s3://$1/gds/

aws s3 cp ~/mozart/ops/opera-pcm/tests/GDS/NISAR_S198_SGS_WG5_M00_P00115_R96_C97_G81_2022_008_09_03_16_000000000.vc03 s3://$1/
aws s3 cp ~/mozart/ops/opera-pcm/tests/GDS/NISAR_S198_SGS_WG5_M00_P00115_R96_C97_G81_2022_008_09_03_16_000000000.vc69 s3://$1/
