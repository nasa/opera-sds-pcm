#!/bin/bash

aws s3 cp ~/mozart/ops/opera-pcm/tests/radar_config/test-files/id_01-00-0701_radar-configuration_v45-14.xml s3://$1/
aws s3 cp ~/mozart/ops/opera-pcm/tests/radar_config/test-files/id_06-00-0701_chirp-parameter_v45-14.xml s3://$1/
aws s3 cp ~/mozart/ops/opera-pcm/tests/radar_config/test-files/id_ff-00-ff01_waveform_v45-14.xml s3://$1/
