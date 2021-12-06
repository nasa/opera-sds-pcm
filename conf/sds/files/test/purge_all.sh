#!/bin/bash
export GRQ_PVT_IP=$(grep ^GRQ_PVT_IP ~/.sds/config | cut -d: -f2 | xargs)
export DATASET_BUCKET=$(grep ^DATASET_BUCKET ~/.sds/config | cut -d: -f2 | xargs)

# delete indices
for i in `curl "${GRQ_PVT_IP}:9200/_cat/indices/grq_*" | awk '{print $3}'`; do
  echo -n "deleting $i..."
  curl -XDELETE "${GRQ_PVT_IP}:9200/${i}"
  echo "done."
done

# delete bucket dirs
aws s3 rm --recursive s3://${DATASET_BUCKET}/products
aws s3 rm --recursive s3://${DATASET_BUCKET}/staging_area
aws s3 rm --recursive s3://${TRIAGE_BUCKET}
aws s3 rm --recursive s3://${LTS_BUCKET}

# remove cached docker images from factotum
fab -f ~/.sds/cluster.py -R factotum remove_docker_images
