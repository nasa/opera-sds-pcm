# Comment the line below if you want to keep the content of LTS bucket which are mainly for Compressed CSLC files
# Generally you do not want to delete them in the OPS venue
# aws s3 rm --recursive s3://opera-ops-lts-fwd &&

aws-login -pub &&
~/terraform destroy -var-file=ops.tfvars &&
aws --profile saml-pub logs describe-log-groups --log-group-name-prefix /aws/lambda/opera-ops-fwd | grep logGroupName | cut -d '"' -f4 | xargs -i -t aws --profile saml-pub logs delete-log-group --log-group-name {} &&
aws --profile saml-pub logs describe-log-groups --log-group-name-prefix /opera/sds/opera-ops-fwd | grep logGroupName | cut -d '"' -f4 | xargs -i -t aws --profile saml-pub logs delete-log-group --log-group-name {} &&
aws --profile saml-pub ec2 describe-launch-templates | grep LaunchTemplateName | grep ops-fwd | cut -d '"' -f4 | xargs -i -t aws --profile saml-pub ec2 delete-launch-template --launch-template-name {}