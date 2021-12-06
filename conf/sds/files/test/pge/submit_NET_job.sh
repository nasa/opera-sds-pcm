
#!/bin/bash
dir=$HOME/mozart/ops/opera-pcm/tests/pge/net

if [ ! -d "$dir" ];
    then
        mkdir $dir
fi

echo "**************** Dowloading and moving NET testdata ****************"
if [ ! -f "L0B_RRSD_REE_NOISEST1.h5" ];
    then
        curl -O "https://cae-artifactory.jpl.nasa.gov/artifactory/general-develop/gov/nasa/jpl/opera/adt/r1.1/data/L0B_RRSD_REE_NOISEST1/L0B_RRSD_REE_NOISEST1.h5"
fi


cp L0B_RRSD_REE_NOISEST1.h5 $dir/NISAR_L0_PR_RRSD_001_001_A_128S_20061008T060900_20061008T061100_D00101_M_001.h5


cp $HOME/mozart/etc/settings.yaml settings_temp.yaml
sed -i 's/\/home\/ops\/verdi/\/export\/home\/hysdsops\/mozart/g' ./settings_temp.yaml

echo "**************** Manually ingesting L0B product ****************"
python ~/mozart/ops/opera-pcm/extractor/extract.py --settings ./settings_temp.yaml --workspace ./output $HOME/mozart/ops/opera-pcm/tests/pge/net/NISAR_L0_PR_RRSD_001_001_A_128S_20061008T060900_20061008T061100_D00101_M_001.h5
python ~/mozart/ops/hysds/scripts/ingest_dataset.py ./output/NISAR_L0_PR_RRSD_001_001_A_128S_20061008T060900_20061008T061100_D00101_M_001 $HOME/mozart/etc/datasets.json
rm -rf ./output/NISAR_L0_PR_RRSD_001_001_A_128S_20061008T060900_20061008T061100_D00101_M_001
echo "**************** Submitting NET job ****************"
python submit_pge_job.py NISAR_L0_PR_RRSD_001_001_A_128S_20061008T060900_20061008T061100_D00101_M_001 --input_dataset_type L0B_L_RRSD  --job_type NET --not_sciflo --release_version $1
