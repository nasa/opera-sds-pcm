# OPERA-terraform
OPERA cluster provisioning using Terraform

## Prerequisites
1. Install Terraform from https://www.terraform.io/. You can run `terraform` from any machine or your laptop. Minimum version required is 0.13.0.
1. If you are using AWS, make sure you have your credentials setup up. To set them up, install the AWS CLI from https://aws.amazon.com/cli/ and run `aws configure`.
1. If you need Temporary AWS keys, follow the instructions at https://wiki.jpl.nasa.gov/display/NISARSDS/Temporary+AWS+Key+Instructions to set that up.

## Usage
1. Clone the repo
   ```
   git clone https://github.com/nasa/opera-sds-pcm.git
   cd cluster_provisioning
   ```
1. If you are provisioning a cluster in the dev AWS account:
   ```
   cd dev
   ```
   If you are provisioning a cluster in the I&T AWS account:
   ```
   cd int
   ```
   If you are provisioning a cluster in the ops AWS account:
   ```
   cd ops
   ```
1. Initialize so plugins are installed:
   ```
   terraform init
   ```
1. Authenticate via JPL's federated services to retrieve your temporary AWS access keys:
   ```
   aws-login-pub.py
   ```
    - Instructions are located at https://github.jpl.nasa.gov/cloud/Access-Key-Generation. This will save your temporary creds under the `saml-pub` profile.
1. If applicable, edit the variables.tf file with custom variables for this installation venue. Many of these values can be acquired from the aws console.
   ```
   vi variables.tf
   ```
    - For ami values, use the recommended amis from SA team
    - If you want to use separate security groups for mozart/metrics/ci/grq/verdi etc, you can create variable "vpc_security_group_id_<instance_name>" but in that case you will have to update aws.tf file to replace the values.
1. Determine the following values for your HySDS cluster. Note that `project`, `venue` and `counter` will be used to uniquely name and identify your cluster's resources:

| Variable          | Description              | Default Value (if any) |
|:-----------------:|:-------------------------|:----------------------:|
| hysds_release     | HySDS core release to install. e.g. v4.0.1-beta.8-oraclelinux, develop. | develop |
| private_key_file  | Path to the PEM key file. e.g. ~/.ssh/pcmdev.pem. |
| project           | Name of the project e.g. nisar, smap, aria, grfn. For OPERA, value is opera. | opera |
| venue             | Venue name e.g. ops, dev, test, hyunlee, chrisjrd, etc. |
| counter           | Integer value e.g. 1, 2, 3. |
| git_auth_key      | github OAUTH token. Can be generated from the GitHub User Interface. |
| jenkins_api_key   | Jenkins API token. Can be generated from the Jenkins User Interface. |
| cluster_security_group_id | The Cluster Security Group ID specific to your user. Can be found under the Security Groups interface in the AWS Console. |
| verdi_security_group_id | The Verdi Security Group ID specific to your user. Can be found under the Security Groups interface in the AWS Console. |
| cnm_r_event_trigger | Specify either 'sns' or 'kinesis' to handle processing of CNM-R messages. | sns |
| pcm_branch | Specify the opera-pcm branch to use in provisioning. | develop |
| product_delivery_branch | Specify the CNM_product_delivery repo branch to use in provisioning. | develop | 
| lambda_package_release | Specify the release version of the Lambda package to deploy. | develop |
| 
1. Validate your configuration:
   ```
   terraform validate --var pcm_branch=<branch_or_tag> --var hysds_release=v4.0.1-beta.8-oraclelinux --var private_key_file=<PEM key file> --var project=opera --var venue=<venue_value> --var counter=<1, 2, 3, ..>  --var git_auth_key=<GIT OAUTH TOKEN> --var jenkins_api_key=<JENKINS API KEY> --var cluster_security_group_id=<user_specific_cluster_security_group_id> --var verdi_security_group_id=<verdi_specific_cluster_security_group_id> --var asg_vpc=<asg_vpc>
   ```
1. Build your OPERA cluster:
   ```
   terraform apply --var pcm_branch=<branch_or_tag> --var hysds_release=v4.0.1-beta.8-oraclelinux --var private_key_file=<PEM key file> --var project=opera --var venue=<venue_value> --var counter=<1, 2, 3, ..>  --var git_auth_key=<GIT OAUTH TOKEN> --var jenkins_api_key=<JENKINS API KEY> --var cluster_security_group_id=<user_specific_cluster_security_group_id> --var verdi_security_group_id=<verdi_specific_cluster_security_group_id> --var asg_vpc=<asg_vpc>
   ```
1. Show status of your OPERA cluster:
   ```
   terraform show
   ```
1. Destroy your OPERA cluster once it's no longer needed:
   ```
   terraform destroy --var pcm_branch=<branch_or_tag> --var hysds_release=v4.0.1-beta.8-oraclelinux --var private_key_file=<PEM key file> --var project=opera --var venue=<venue_value> --var counter=<1, 2, 3, ..>  --var git_auth_key=<GIT OAUTH TOKEN> --var jenkins_api_key=<JENKINS API KEY> --var cluster_security_group_id=<user_specific_cluster_security_group_id> --var verdi_security_group_id=<verdi_specific_cluster_security_group_id> --var asg_vpc=<asg_vpc>
   ```
