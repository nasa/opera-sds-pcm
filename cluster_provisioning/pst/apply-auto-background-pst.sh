aws-login -pub
nohup ~/terraform apply -var-file=pst.tfvars --auto-approve &
tail -f nohup.out

