aws-login -pub
nohup ~/terraform apply -var-file=pst.tfvars --auto-approve &
sleep 2
tail -f nohup.out

