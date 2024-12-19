aws-login -pub &&
~/terraform destroy -var-file=dev-common.tfvars &&
aws --profile saml-pub logs describe-log-groups --log-group-name-prefix /aws/lambda/opera-dev-fwd | grep logGroupName | cut -d '"' -f4 | xargs -i -t aws --profile saml-pub logs delete-log-group --log-group-name {} &&
aws --profile saml-pub logs describe-log-groups --log-group-name-prefix /opera/sds/opera-dev-fwd | grep logGroupName | cut -d '"' -f4 | xargs -i -t aws --profile saml-pub logs delete-log-group --log-group-name {} &&
aws --profile saml-pub ec2 describe-launch-templates | grep LaunchTemplateName | grep dev-fwd | cut -d '"' -f4 | xargs -i -t aws --profile saml-pub ec2 delete-launch-template --launch-template-name {}

# Comment the line below if you want to keep the content of LTS bucket which are mainly for Compressed CSLC files
aws --profile saml-pub s3 rm --recursive s3://opera-dev-lts-fwd/products/CSLC_S1_COMPRESSED