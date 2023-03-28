aws-login -pub
nohup ~/terraform apply -var-file=ops.tfvars --auto-approve &
tail -f nohup.out

