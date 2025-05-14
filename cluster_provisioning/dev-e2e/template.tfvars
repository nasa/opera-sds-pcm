keypair_name = "username"
private_key_file = "pem file location"
venue = "ci"
counter = "1"
hysds_release = "v5.0.1"

pcm_branch = "develop"
pcm_commons_branch = "3.1.4"
product_delivery_branch = "develop"
bach_api_branch = "develop"
bach_ui_branch = "develop"
lambda_package_release = "develop"

amis = {
    # HySDS v5.0.1 - May 12, 2025 - R3.1
    mozart    = "ami-xyz" # mozart v4.26 - 250512
    metrics   = "ami-xyz" # metrics v4.18 - 250512
    grq       = "ami-xyz" # grq v4.19 - 250512
    factotum  = "ami-xyz" # factotum v4.17 - 250512
    autoscale = "ami-xyz" # verdi v4.17 patchdate - 250512
}

cnm_r_sqs_arn = {
  dev  = "dev arn"
  int  = "int arn"
  test = "test arn"
  prod = "prod arn"
}

asf_cnm_s_id_dev     = "dev account id"
asf_cnm_s_id_dev_int = "dev-int account id"
asf_cnm_s_id_test    = "test account id"
asf_cnm_s_id_prod    = "prod account id"

aws_account_id = "current account"
public_verdi_security_group_id = "sg-xyz"
private_verdi_security_group_id = "sg-xyiz"

cluster_security_group_id = "sg-xxxxxxxxx"

subnet_id = "subnet-xxxxx"
lambda_vpc = "vpc-xxxx"
public_asg_vpc = "vpc-xxxxxxx"
private_asg_vpc = "vpc-xxxxxxx"

lambda_role_arn = "lamba role arn"
es_bucket_role_arn = "es role arn"

po_daac_delivery_proxy = "sns arn"
asf_daac_delivery_proxy = "sqs arn"

grq_aws_es_host = "vpce-xxxxxxxx"
grq_aws_es_host_private_verdi = "vpce-xxxxxxxxx"

dataspace_user = "dataspace user" 
dataspace_pass = "dataspace pass"
earthdata_user = "earthdata user for prod"
earthdata_pass = "earthdata pass for prod"
earthdata_uat_user = "earthdata user for uat"
earthdata_uat_pass = "earthdata pass for uat"
artifactory_fn_user = "artifactory username"
artifactory_fn_api_key = "artifactory api key"

# for github.jpl.nasa.gov
git_auth_key = "xxxxxx"
jenkins_api_user = "username"
jenkins_api_key = "api key"
es_user = "user"
es_pass = "pass"
ops_password = "some pass"
