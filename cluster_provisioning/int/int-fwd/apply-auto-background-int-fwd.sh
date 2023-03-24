aws-login -pub
nohup ~/terraform apply -var-file=int.tfvars --auto-approve &
tail -f nohup.out

