# Comment the line below if you want to keep the content of LTS bucket which are mainly for Compressed CSLC files
aws s3 rm --recursive s3://opera-int-lts-fwd &&

aws-login -pub &&
~/terraform destroy -var-file=int.tfvars &&
aws --profile saml-pub logs describe-log-groups --log-group-name-prefix /aws/lambda/opera-int-fwd | grep logGroupName | cut -d '"' -f4 | xargs -i -t aws --profile saml-pub logs delete-log-group --log-group-name {} &&
aws --profile saml-pub logs describe-log-groups --log-group-name-prefix /opera/sds/opera-int-fwd | grep logGroupName | cut -d '"' -f4 | xargs -i -t aws --profile saml-pub logs delete-log-group --log-group-name {} &&
aws --profile saml-pub ec2 describe-launch-templates | grep LaunchTemplateName | grep int-fwd | cut -d '"' -f4 | xargs -i -t aws --profile saml-pub ec2 delete-launch-template --launch-template-name {}

