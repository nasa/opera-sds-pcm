aws-login -pub
nohup ~/terraform apply -var-file=int.tfvars --auto-approve &
sleep 2
tail -f nohup.out

