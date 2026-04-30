#!/bin/bash
export GRQ_PVT_IP=$(grep ^GRQ_PVT_IP ~/.sds/config | cut -d: -f2 | xargs)
export DATASET_BUCKET=$(grep ^DATASET_BUCKET ~/.sds/config | cut -d: -f2 | xargs)

# delete indices
for i in `curl -k --netrc-file ~/.netrc-os "https://${GRQ_PVT_IP}:9200/_cat/indices/grq_*" | awk '{print $3}'`; do
  echo -n "deleting $i..."
  curl -k --netrc-file ~/.netrc-os -XDELETE "https://${GRQ_PVT_IP}:9200/${i}"
  echo "done."
done

# delete bucket dirs
aws s3 rm --recursive s3://${DATASET_BUCKET}/products
aws s3 rm --recursive s3://${DATASET_BUCKET}/inputs

