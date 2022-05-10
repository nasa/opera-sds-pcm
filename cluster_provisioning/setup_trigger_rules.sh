#!/bin/bash
source $HOME/.bash_profile

# fail on any errors
set -ex

curl -XDELETE http://$(curl http://169.254.169.254/latest/meta-data/local-ipv4):9200/user_rules-grq
curl -XDELETE http://$(curl http://169.254.169.254/latest/meta-data/local-ipv4):9200/user_rules-mozart
fab -f ~/.sds/cluster.py -R mozart,grq create_all_user_rules_index

# import some tosca + figaro rules
pushd ~/.sds/files/test
./import_rules.sh
./import_rules-mozart.sh
./import_product_delivery_rules.sh
popd
