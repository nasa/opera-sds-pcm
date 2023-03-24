aws-login -pub
nohup ~/terraform apply -var-file=dev-common.tfvars --auto-approve &
tail -f nohup.out

