######################
# mozart
######################
locals {
  q_config = <<EOT
QUEUES:
    %{~for queue, queue_config in var.queues~}
  - QUEUE_NAME: ${queue}
    INSTANCE_TYPES:
    %{~for instance_type in queue_config["instance_type"]~}
      - ${instance_type}
    %{~endfor~}
    TOTAL_JOBS_METRIC: ${queue_config["total_jobs_metric"]}
    %{~endfor~}
  EOT
}

resource "aws_instance" "mozart" {
  depends_on           = [aws_instance.metrics, aws_autoscaling_group.autoscaling_group]
  ami                  = var.amis["mozart"]
  instance_type        = var.mozart["instance_type"]
  key_name             = local.key_name
  availability_zone    = var.az
  iam_instance_profile = var.pcm_cluster_role["name"]
  private_ip           = var.mozart["private_ip"] != "" ? var.mozart["private_ip"] : null
  user_data            = <<-EOT
              #!/bin/bash

              FACTOTUMIP=${aws_instance.factotum.private_ip}
              GRQIP=${aws_instance.grq.private_ip}
              METRICSIP=${aws_instance.metrics.private_ip}
              PROJECT=${var.project}
              ENVIRONMENT=${var.environment}

              echo "PASS" >> /tmp/user_data_test.txt

              mkdir -p /opt/aws/amazon-cloudwatch-agent/etc/
              touch /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
              echo '{
                "agent": {
                  "metrics_collection_interval": 10,
                  "logfile": "/opt/aws/amazon-cloudwatch-agent/logs/amazon-cloudwatch-agent.log"
                },
                "logs": {
                  "logs_collected": {
                    "files": {
                      "collect_list": [
                        {
                          "file_path": "/opt/aws/amazon-cloudwatch-agent/logs/amazon-cloudwatch-agent.log",
                          "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/amazon-cloudwatch-agent.log",
                          "timezone": "UTC"
                        }
                      ]
                    }
                  },
                  "force_flush_interval" : 15
                }
              }' > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
              /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
              EOT
  tags = {
    Name  = "${var.project}-${var.venue}-${local.counter}-pcm-${var.mozart["name"]}",
    Bravo = "pcm"
  }
  volume_tags = {
    Bravo = "pcm"
  }
  #This is very important, as it tells terraform to not mess with tags
  lifecycle {
    #    ignore_changes = [tags]
    ignore_changes = [tags, volume_tags]
  }
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [var.cluster_security_group_id]

  root_block_device {
    volume_size           = var.mozart["root_dev_size"]
    volume_type           = "gp2"
    delete_on_termination = true
  }

  connection {
    type        = "ssh"
    host        = aws_instance.mozart.private_ip
    user        = "hysdsops"
    private_key = file(var.private_key_file)
  }

  provisioner "local-exec" {
    command = "echo export MOZART_IP=${aws_instance.mozart.private_ip} > mozart_ip.sh"
  }

  provisioner "file" {
    source      = var.private_key_file
    destination = ".ssh/${basename(var.private_key_file)}"
  }

  provisioner "file" {
    content     = templatefile("${path.module}/bash_profile.mozart.tmpl", {})
    destination = ".bash_profile"
  }

  provisioner "file" {
    content     = local.q_config
    destination = "q_config"
  }

  provisioner "file" {
    source      = "${path.module}/../../../tools/download_artifact.sh"
    destination = "download_artifact.sh"
  }

  provisioner "remote-exec" {
    inline = [<<-EOT
      while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 10; done
      set -ex
      chmod 755 ~/download_artifact.sh
      chmod 400 ~/.ssh/${basename(var.private_key_file)}
      mkdir ~/.sds

      for i in {1..18}; do
        if [[ `grep "redis single-password" ~/.creds` != "" ]]; then
          echo "redis password found in ~/.creds"
          break
        else
          echo "redis password NOT found in ~/.creds, sleeping 10 sec."
          sleep 10
        fi
      done

      scp -o StrictHostKeyChecking=no -q -i ~/.ssh/${basename(var.private_key_file)} hysdsops@${aws_instance.metrics.private_ip}:~/.creds ~/.creds_metrics
      echo TYPE: hysds > ~/.sds/config
      echo >> ~/.sds/config

      echo MOZART_PVT_IP: ${aws_instance.mozart.private_ip} >> ~/.sds/config
      echo MOZART_PUB_IP: ${aws_instance.mozart.private_ip} >> ~/.sds/config
      echo MOZART_FQDN: ${aws_instance.mozart.private_ip} >> ~/.sds/config
      echo >> ~/.sds/config

      echo MOZART_RABBIT_PVT_IP: ${aws_instance.mozart.private_ip} >> ~/.sds/config
      echo MOZART_RABBIT_PUB_IP: ${aws_instance.mozart.private_ip} >> ~/.sds/config
      echo MOZART_RABBIT_FQDN: ${aws_instance.mozart.private_ip} >> ~/.sds/config
      echo MOZART_RABBIT_USER: $(awk 'NR==1{print $2; exit}' .creds) >> ~/.sds/config
      echo MOZART_RABBIT_PASSWORD: $(awk 'NR==1{print $3; exit}' .creds)>> ~/.sds/config
      echo >> ~/.sds/config

      echo MOZART_REDIS_PVT_IP: ${aws_instance.mozart.private_ip} >> ~/.sds/config
      echo MOZART_REDIS_PUB_IP: ${aws_instance.mozart.private_ip} >> ~/.sds/config
      echo MOZART_REDIS_FQDN: ${aws_instance.mozart.private_ip} >> ~/.sds/config
      echo MOZART_REDIS_PASSWORD: $(awk 'NR==2{print $3; exit}' .creds) >> ~/.sds/config
      echo >> ~/.sds/config

      echo MOZART_ES_PVT_IP: ${aws_instance.mozart.private_ip} >> ~/.sds/config
      echo MOZART_ES_PUB_IP: ${aws_instance.mozart.private_ip} >> ~/.sds/config
      echo MOZART_ES_FQDN: ${aws_instance.mozart.private_ip} >> ~/.sds/config
      echo OPS_USER: hysdsops >> ~/.sds/config
      echo OPS_HOME: $${HOME} >> ~/.sds/config
      echo OPS_PASSWORD_HASH: $(echo -n ${var.ops_password} | sha224sum |awk '{ print $1}') >> ~/.sds/config
      echo LDAP_GROUPS: ${var.project}-pcm-dev >> ~/.sds/config
      echo KEY_FILENAME: $${HOME}/.ssh/${basename(var.private_key_file)} >> ~/.sds/config
      echo JENKINS_USER: jenkins >> ~/.sds/config
      echo JENKINS_DIR: /var/lib/jenkins >> ~/.sds/config
      echo >> ~/.sds/config

      echo METRICS_PVT_IP: ${aws_instance.metrics.private_ip} >> ~/.sds/config
      echo METRICS_PUB_IP: ${aws_instance.metrics.private_ip} >> ~/.sds/config
      echo METRICS_FQDN: ${aws_instance.metrics.private_ip} >> ~/.sds/config
      echo >> ~/.sds/config

      echo METRICS_REDIS_PVT_IP: ${aws_instance.metrics.private_ip} >> ~/.sds/config
      echo METRICS_REDIS_PUB_IP: ${aws_instance.metrics.private_ip} >> ~/.sds/config
      echo METRICS_REDIS_FQDN: ${aws_instance.metrics.private_ip} >> ~/.sds/config
      echo METRICS_REDIS_PASSWORD: $(awk 'NR==1{print $3; exit}' .creds_metrics) >> ~/.sds/config
      echo >> ~/.sds/config

      echo METRICS_ES_PVT_IP: ${aws_instance.metrics.private_ip} >> ~/.sds/config
      echo METRICS_ES_PUB_IP: ${aws_instance.metrics.private_ip} >> ~/.sds/config
      echo METRICS_ES_FQDN: ${aws_instance.metrics.private_ip} >> ~/.sds/config
      echo >> ~/.sds/config

      echo GRQ_PVT_IP: ${aws_instance.grq.private_ip} >> ~/.sds/config
      echo GRQ_PUB_IP: ${aws_instance.grq.private_ip} >> ~/.sds/config
      echo GRQ_FQDN: ${aws_instance.grq.private_ip} >> ~/.sds/config
      echo GRQ_PORT: 8878 >> ~/.sds/config
      echo >> ~/.sds/config

      echo GRQ_AWS_ES: ${var.grq_aws_es ? var.grq_aws_es : false} >> ~/.sds/config
      echo GRQ_ES_PROTOCOL: ${var.grq_aws_es ? "https" : "http"} >> ~/.sds/config
      echo GRQ_ES_PVT_IP: ${var.grq_aws_es ? var.grq_aws_es_host : aws_instance.grq.private_ip} >> ~/.sds/config
      echo GRQ_ES_PUB_IP: ${var.grq_aws_es ? var.grq_aws_es_host : aws_instance.grq.private_ip} >> ~/.sds/config
      echo GRQ_ES_FQDN: ${var.grq_aws_es ? var.grq_aws_es_host : aws_instance.grq.private_ip} >> ~/.sds/config
      echo GRQ_ES_PORT: ${var.grq_aws_es ? var.grq_aws_es_port : 9200} >> ~/.sds/config
      echo >> ~/.sds/config

      if [ "${var.grq_aws_es}" = true ] && [ "${var.use_grq_aws_es_private_verdi}" = true ]; then
        echo GRQ_AWS_ES_PRIVATE_VERDI: ${var.grq_aws_es_host_private_verdi} >> ~/.sds/config
        echo GRQ_ES_PVT_IP_VERDI: ${var.grq_aws_es_host_private_verdi} >> ~/.sds/config
        echo GRQ_ES_PUB_IP_VERDI: ${var.grq_aws_es_host_private_verdi} >> ~/.sds/config
        echo GRQ_ES_FQDN_PVT_IP_VERDI: ${var.grq_aws_es_host_private_verdi} >> ~/.sds/config
        echo ARTIFACTORY_REPO: ${var.artifactory_repo} >> ~/.sds/config
        echo >> ~/.sds/config
      fi

      echo FACTOTUM_PVT_IP: ${aws_instance.factotum.private_ip} >> ~/.sds/config
      echo FACTOTUM_PUB_IP: ${aws_instance.factotum.private_ip} >> ~/.sds/config
      echo FACTOTUM_FQDN: ${aws_instance.factotum.private_ip} >> ~/.sds/config
      echo >> ~/.sds/config

      echo CI_PVT_IP: ${var.common_ci["private_ip"]} >> ~/.sds/config
      echo CI_PUB_IP: ${var.common_ci["private_ip"]} >> ~/.sds/config
      echo CI_FQDN: ${var.common_ci["private_ip"]} >> ~/.sds/config
      echo >> ~/.sds/config

      echo JENKINS_HOST: ${var.jenkins_host} >> ~/.sds/config
      echo JENKINS_ENABLED: ${var.jenkins_enabled} >> ~/.sds/config
      echo JENKINS_API_USER: ${var.jenkins_api_user != "" ? var.jenkins_api_user : var.venue} >> ~/.sds/config
      echo JENKINS_API_KEY: ${var.jenkins_api_key} >> ~/.sds/config
      echo >> ~/.sds/config

      echo VERDI_PVT_IP: ${var.common_ci["private_ip"]} >> ~/.sds/config
      echo VERDI_PUB_IP: ${var.common_ci["private_ip"]} >> ~/.sds/config
      echo VERDI_FQDN: ${var.common_ci["private_ip"]} >> ~/.sds/config
      echo OTHER_VERDI_HOSTS: >> ~/.sds/config
      echo '  - VERDI_PVT_IP:' >> ~/.sds/config
      echo '    VERDI_PUB_IP:' >> ~/.sds/config
      echo '    VERDI_FQDN:' >> ~/.sds/config
      echo >> ~/.sds/config

      echo DAV_SERVER: None >> ~/.sds/config
      echo DAV_USER: None >> ~/.sds/config
      echo DAV_PASSWORD: None >> ~/.sds/config
      echo >> ~/.sds/config

      echo DATASET_AWS_REGION: us-west-2 >> ~/.sds/config
      echo DATASET_AWS_ACCESS_KEY: >> ~/.sds/config
      echo DATASET_AWS_SECRET_KEY: >> ~/.sds/config
      echo DATASET_S3_ENDPOINT: s3-us-west-2.amazonaws.com >> ~/.sds/config
      echo DATASET_S3_WEBSITE_ENDPOINT: s3-website-us-west-2.amazonaws.com >> ~/.sds/config
      echo DATASET_BUCKET: ${local.dataset_bucket} >> ~/.sds/config
      echo OSL_BUCKET: ${local.osl_bucket} >> ~/.sds/config
      echo TRIAGE_BUCKET: ${local.triage_bucket} >> ~/.sds/config
      echo LTS_BUCKET: ${local.lts_bucket} >> ~/.sds/config
      echo >> ~/.sds/config

      echo AWS_REGION: us-west-2 >> ~/.sds/config
      echo AWS_ACCESS_KEY: >> ~/.sds/config
      echo AWS_SECRET_KEY: >> ~/.sds/config
      echo S3_ENDPOINT: s3-us-west-2.amazonaws.com >> ~/.sds/config
      echo CODE_BUCKET: ${local.code_bucket} >> ~/.sds/config
      echo VERDI_PRIMER_IMAGE: s3://${local.code_bucket}/hysds-verdi-${var.hysds_release}.tar.gz >> ~/.sds/config
      echo VERDI_TAG: ${var.hysds_release} >> ~/.sds/config
      echo VERDI_UID: 1002 >> ~/.sds/config
      echo VERDI_GID: 1002 >> ~/.sds/config
      echo VENUE: ${var.project}-${var.venue}-${local.counter} >> ~/.sds/config
      echo >> ~/.sds/config

      echo ASG: >> ~/.sds/config
      echo '  AMI: ${var.amis["autoscale"]}' >> ~/.sds/config
      echo '  KEYPAIR: ${local.key_name}' >> ~/.sds/config
      echo '  USE_ROLE: ${var.asg_use_role}' >> ~/.sds/config
      echo '  ROLE: ${var.asg_role}' >> ~/.sds/config

      echo STAGING_AREA: >> ~/.sds/config
      echo '  LAMBDA_SECURITY_GROUPS:' >> ~/.sds/config
      echo '    - ${var.cluster_security_group_id}' >> ~/.sds/config
      echo '  LAMBDA_VPC: ${var.lambda_vpc}' >> ~/.sds/config
      echo '  LAMBDA_ROLE: "${var.lambda_role_arn}"' >> ~/.sds/config
      echo '  JOB_RELEASE: ${var.pcm_branch}' >> ~/.sds/config
      echo >> ~/.sds/config

      echo CNM_RESPONSE_HANDLER: >> ~/.sds/config
      echo '  LAMBDA_SECURITY_GROUPS:' >> ~/.sds/config
      echo '    - ${var.cluster_security_group_id}' >> ~/.sds/config
      echo '  LAMBDA_VPC: ${var.lambda_vpc}' >> ~/.sds/config
      echo '  LAMBDA_ROLE: "${var.lambda_role_arn}"' >> ~/.sds/config
      echo '  JOB_TYPE: "${var.cnm_r_handler_job_type}"' >> ~/.sds/config
      echo '  JOB_RELEASE: ${var.pcm_branch}' >> ~/.sds/config
      echo '  JOB_QUEUE: ${var.cnm_r_job_queue}' >> ~/.sds/config
      echo '  PO_DAAC_CNM_R_EVENT_TRIGGER: ${var.po_daac_cnm_r_event_trigger}' >> ~/.sds/config
      echo '  ASF_DAAC_CNM_R_EVENT_TRIGGER: ${var.asf_daac_cnm_r_event_trigger}' >> ~/.sds/config
      echo '  PRODUCT_TAG: true' >> ~/.sds/config
      echo '  ALLOWED_ACCOUNT: "${var.cnm_r_allowed_account}"' >> ~/.sds/config
      echo >> ~/.sds/config

      echo GIT_OAUTH_TOKEN: ${var.git_auth_key} >> ~/.sds/config
      echo >> ~/.sds/config

      echo PROVES_URL: https://prov-es.jpl.nasa.gov/beta >> ~/.sds/config
      echo PROVES_IMPORT_URL: https://prov-es.jpl.nasa.gov/beta/api/v0.1/prov_es/import/json >> ~/.sds/config
      echo DATASETS_CFG: $${HOME}/verdi/etc/datasets.json >> ~/.sds/config
      echo >> ~/.sds/config

      echo SYSTEM_JOBS_QUEUE: system-jobs-queue >> ~/.sds/config
      echo >> ~/.sds/config

      echo MOZART_ES_CLUSTER: resource_cluster >> ~/.sds/config
      echo METRICS_ES_CLUSTER: metrics_cluster >> ~/.sds/config
      echo DATASET_QUERY_INDEX: grq >> ~/.sds/config
      echo USER_RULES_DATASET_INDEX: user_rules >> ~/.sds/config
      echo EXTRACTOR_HOME: /home/ops/verdi/ops/${var.project}-pcm/extractor >> ~/.sds/config
      echo CONTAINER_REGISTRY: localhost:5050 >> ~/.sds/config
      echo CONTAINER_REGISTRY_BUCKET: ${var.docker_registry_bucket} >> ~/.sds/config

      echo USE_S3_URI: "${var.use_s3_uri_structure}" >> ~/.sds/config

      echo PO_DAAC_PROXY: "${var.po_daac_delivery_proxy}" >> ~/.sds/config
      if [ "${local.po_daac_delivery_event_type}" = "sqs" ]; then
        echo PO_DAAC_SQS_URL: "https://sqs.${local.po_daac_delivery_region}.amazonaws.com/${local.po_daac_delivery_account}/${local.po_daac_delivery_resource_name}" >> ~/.sds/config
        echo PO_DAAC_ENDPOINT_URL: "${var.po_daac_endpoint_url}" >> ~/.sds/config
      else
        echo PO_DAAC_SQS_URL: "" >> ~/.sds/config
      fi

      echo ASF_DAAC_PROXY: "${var.asf_daac_delivery_proxy}" >> ~/.sds/config

      if [ "${local.asf_daac_delivery_event_type}" = "sqs" ]; then
        echo ASF_DAAC_SQS_URL: "https://sqs.${local.asf_daac_delivery_region}.amazonaws.com/${local.asf_daac_delivery_account}/${local.asf_daac_delivery_resource_name}" >> ~/.sds/config
        echo ASF_DAAC_ENDPOINT_URL: "${var.asf_daac_endpoint_url}" >> ~/.sds/config
      else
        echo ASF_DAAC_SQS_URL: "" >> ~/.sds/config
      fi

      echo TRACE: "${var.trace}" >> ~/.sds/config
      echo PRODUCT_DELIVERY_REPO: "${var.product_delivery_repo}" >> ~/.sds/config
      echo PRODUCT_DELIVERY_BRANCH: "${var.product_delivery_branch}" >> ~/.sds/config
      echo PCM_COMMONS_REPO: "${var.pcm_commons_repo}" >> ~/.sds/config
      echo PCM_COMMONS_BRANCH: "${var.pcm_commons_branch}" >> ~/.sds/config
      echo CRID: "${var.crid}" >> ~/.sds/config
      cat ~/q_config >> ~/.sds/config
      echo >> ~/.sds/config

      echo INACTIVITY_THRESHOLD: ${var.inactivity_threshold} >> ~/.sds/config
      echo >> ~/.sds/config

      echo 'DATASPACE_USER: "${var.dataspace_user}"' >> ~/.sds/config
      echo 'DATASPACE_PASS: "${var.dataspace_pass}"' >> ~/.sds/config
      echo >> ~/.sds/config

      echo EARTHDATA_USER: ${var.earthdata_user} >> ~/.sds/config
      echo EARTHDATA_PASS: ${var.earthdata_pass} >> ~/.sds/config

      echo EARTHDATA_UAT_USER: ${var.earthdata_uat_user} >> ~/.sds/config
      echo EARTHDATA_UAT_PASS: ${var.earthdata_uat_pass} >> ~/.sds/config
      echo >> ~/.sds/config
    EOT
    ]
  }

  provisioner "remote-exec" {
    inline = [<<-EOT
      while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 10; done
      set -ex
      mv ~/.sds ~/.sds.bak
      rm -rf ~/mozart

      if [ "${var.hysds_release}" = "develop" ]; then
        git clone --quiet --single-branch -b ${var.hysds_release} https://${var.git_auth_key}@github.jpl.nasa.gov/IEMS-SDS/pcm-releaser.git
        cd pcm-releaser
        export release=${var.hysds_release}
        export conda_dir=$HOME/conda
        ./build_conda.sh $conda_dir $release
        cd ..
        rm -rf pcm-releaser

        scp -o StrictHostKeyChecking=no -q -i ~/.ssh/${basename(var.private_key_file)} hysds-conda_env-${var.hysds_release}.tar.gz hysdsops@${aws_instance.metrics.private_ip}:hysds-conda_env-${var.hysds_release}.tar.gz
        ssh -o StrictHostKeyChecking=no -q -i ~/.ssh/${basename(var.private_key_file)} hysdsops@${aws_instance.metrics.private_ip} \
      '
      mkdir -p ~/conda;
      tar xfz hysds-conda_env-${var.hysds_release}.tar.gz -C conda;
      export PATH=$HOME/conda/bin:$PATH;
      conda-unpack;
      rm -rf hysds-conda_env-${var.hysds_release}.tar.gz
      '

        scp -o StrictHostKeyChecking=no -q -i ~/.ssh/${basename(var.private_key_file)} hysds-conda_env-${var.hysds_release}.tar.gz hysdsops@${aws_instance.grq.private_ip}:hysds-conda_env-${var.hysds_release}.tar.gz
        ssh -o StrictHostKeyChecking=no -q -i ~/.ssh/${basename(var.private_key_file)} hysdsops@${aws_instance.grq.private_ip} \
      '
      mkdir -p ~/conda;
      tar xfz hysds-conda_env-${var.hysds_release}.tar.gz -C conda;
      export PATH=$HOME/conda/bin:$PATH;
      conda-unpack;
      rm -rf hysds-conda_env-${var.hysds_release}.tar.gz
      '

        scp -o StrictHostKeyChecking=no -q -i ~/.ssh/${basename(var.private_key_file)} hysds-conda_env-${var.hysds_release}.tar.gz hysdsops@${aws_instance.factotum.private_ip}:hysds-conda_env-${var.hysds_release}.tar.gz
        ssh -o StrictHostKeyChecking=no -q -i ~/.ssh/${basename(var.private_key_file)} hysdsops@${aws_instance.factotum.private_ip} \
      '
      mkdir -p ~/conda;
      tar xfz hysds-conda_env-${var.hysds_release}.tar.gz -C conda;
      export PATH=$HOME/conda/bin:$PATH;
      conda-unpack;
      echo installing gdal for manual execution of daac_data_subscriber.py 
      conda install conda gdal==3.6.4 poppler --yes --quiet
      # take too long to deploy a cluster, need to check more to switch to conda-forge channel
      #conda install -y -c conda-forge conda gdal==3.6.4 poppler --yes --quiet 

      rm -rf hysds-conda_env-${var.hysds_release}.tar.gz
      '
        git clone --quiet --single-branch -b ${var.hysds_release} https://github.com/hysds/hysds-framework

        ./install.sh mozart -d
        rm -rf ~/mozart/pkgs/hysds-verdi-latest.tar.gz
     else
        ~/download_artifact.sh -m "${var.artifactory_mirror_url}" -b "${var.artifactory_base_url}" -k "${var.artifactory_fn_api_key}" "${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/${var.hysds_release}/hysds-conda_env-${var.hysds_release}.tar.gz"
        mkdir -p ~/conda
        tar xfz hysds-conda_env-${var.hysds_release}.tar.gz -C conda
        export PATH=$HOME/conda/bin:$PATH
        conda-unpack
        echo installing gdal for manual execution of daac_data_subscriber.py
        conda install conda gdal==3.6.4 poppler --yes --quiet
        # take too long to deploy a cluster, need to check more to switch to conda-forge channel
        #conda install -y -c conda-forge conda gdal==3.6.4 poppler --yes --quiet

        rm -rf hysds-conda_env-${var.hysds_release}.tar.gz

        ~/download_artifact.sh -m "${var.artifactory_mirror_url}" -b "${var.artifactory_base_url}" -k "${var.artifactory_fn_api_key}" "${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/${var.hysds_release}/hysds-mozart_venv-${var.hysds_release}.tar.gz"
        tar xfz hysds-mozart_venv-${var.hysds_release}.tar.gz
        rm -rf hysds-mozart_venv-${var.hysds_release}.tar.gz

        ~/download_artifact.sh -m "${var.artifactory_mirror_url}" -b "${var.artifactory_base_url}" -k "${var.artifactory_fn_api_key}" "${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/${var.hysds_release}/hysds-verdi_venv-${var.hysds_release}.tar.gz"
        tar xfz hysds-verdi_venv-${var.hysds_release}.tar.gz
        rm -rf hysds-verdi_venv-${var.hysds_release}.tar.gz
      fi
      cd ~/mozart/ops
      if [ "${var.use_artifactory}" = true ]; then

        ~/download_artifact.sh -m "${var.artifactory_mirror_url}" -b "${var.artifactory_base_url}" "${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/${var.project}-sds-pcm-${var.pcm_branch}.tar.gz"
        tar xfz ${var.project}-sds-pcm-${var.pcm_branch}.tar.gz
        ln -s /export/home/hysdsops/mozart/ops/${var.project}-sds-pcm-${var.pcm_branch} /export/home/hysdsops/mozart/ops/${var.project}-pcm
        rm -rf ${var.project}-sds-pcm-${var.pcm_branch}.tar.gz

        ~/download_artifact.sh -m "${var.artifactory_mirror_url}" -b "${var.artifactory_base_url}" "${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/CNM_product_delivery-${var.product_delivery_branch}.tar.gz"
        tar xfz CNM_product_delivery-${var.product_delivery_branch}.tar.gz
        ln -s /export/home/hysdsops/mozart/ops/CNM_product_delivery-${var.product_delivery_branch} /export/home/hysdsops/mozart/ops/CNM_product_delivery
        rm -rf CNM_product_delivery-${var.product_delivery_branch}.tar.gz

        ~/download_artifact.sh -m "${var.artifactory_mirror_url}" -b "${var.artifactory_base_url}" "${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/pcm_commons-${var.pcm_commons_branch}.tar.gz"
        tar xfz pcm_commons-${var.pcm_commons_branch}.tar.gz
        ln -s /export/home/hysdsops/mozart/ops/pcm_commons-${var.pcm_commons_branch} /export/home/hysdsops/mozart/ops/pcm_commons
        rm -rf pcm_commons-${var.pcm_commons_branch}.tar.gz
      else
        git clone --quiet --single-branch -b ${var.pcm_branch} https://${var.git_auth_key}@${var.pcm_repo} ${var.project}-pcm
        git clone --quiet --single-branch -b ${var.product_delivery_branch} https://${var.git_auth_key}@${var.product_delivery_repo}
        git clone --quiet --single-branch -b ${var.pcm_commons_branch} https://${var.git_auth_key}@${var.pcm_commons_repo}
      fi

      cp -rp ${var.project}-pcm/conf/sds ~/.sds
      cp ~/.sds.bak/config ~/.sds
    EOT
    ]
  }



  # Copy down latest opera-sds-int and opera-sds-ops repos for convenience
  provisioner "remote-exec" {
    inline = [<<-EOT
      while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 10; done
      set -ex
      cd ~/mozart/ops
      wget https://github.com/nasa/opera-sds-int/archive/refs/heads/main.zip -O opera-sds-int.zip
      wget https://github.com/nasa/opera-sds-ops/archive/refs/heads/main.zip -O opera-sds-ops.zip
      unzip opera-sds-int.zip
      unzip opera-sds-ops.zip
    EOT
    ]
  }

  provisioner "remote-exec" {
    inline = [<<-EOT
      while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 10; done
      set -ex
      cd ~/mozart/ops
      if [ "${var.grq_aws_es}" = true ]; then
        cp -f ~/.sds/files/supervisord.conf.grq.aws_es ~/.sds/files/supervisord.conf.grq
      fi
      if [ "${var.factotum["instance_type"]}" = "r6i.4xlarge" ]; then
        cp -f ~/.sds/files/supervisord.conf.factotum.small_instance ~/.sds/files/supervisord.conf.factotum
      elif [ "${var.factotum["instance_type"]}" = "r5.8xlarge" ]; then
        cp -f ~/.sds/files/supervisord.conf.factotum.large_instance ~/.sds/files/supervisord.conf.factotum
      fi
    EOT
    ]
  }

  # To test HySDS core development (feature branches), uncomment this block
  # and add lines to perform the mods to test them. Three examples have been
  # left as described below:
  #provisioner "remote-exec" {
  #  inline = [
  #    "set -ex",
  #    "source ~/.bash_profile",

  # Example 1: test a single file update from an sdscli feature branch named hotfix-sighup
  #    "cd ~/mozart/ops/sdscli/sdscli/adapters/hysds",
  #    "mv fabfile.py fabfile.py.bak",
  #    "wget https://raw.githubusercontent.com/sdskit/sdscli/hotfix-sighup/sdscli/adapters/hysds/fabfile.py",

  # Example 2: test an entire feature branch (need HYSDS_RELEASE=develop terraform variable)
  #    "cd ~/mozart/ops/hysds",
  #    "git checkout <dustins_branch>",
  #    "pip install -e .",

  # Example 3: test a custom verdi docker image on the ASGs (need HYSDS_RELEASE=develop terraform variable)
  #    "cd ~/mozart/pkgs",
  #    "mv hysds-verdi-develop.tar.gz hysds-verdi-develop.tar.gz.bak",
  #    "docker pull hysds/verdi:<dustins_branch>",
  #    "docker tag hysds/verdi:<dustins_branch> hysds/verdi:develop",
  #    "docker save hysds/verdi:develop > hysds-verdi-develop.tar",
  #    "pigz hysds-verdi-develop.tar",

  #  ]
  #}

  provisioner "remote-exec" {
    inline = [<<-EOT
     while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 10; done
      set -ex
      source ~/.bash_profile
      if [ "${var.hysds_release}" = "develop" ]; then
        sds -d update mozart -f
        sds -d update grq -f
        sds -d update metrics -f
        sds -d update factotum -f
      else
        sds -d update mozart -f -c
        sds -d update grq -f -c
        sds -d update metrics -f -c
        sds -d update factotum -f -c
      fi
      cp -pr ~/mozart/ops/opera-pcm ~/verdi/ops/opera-pcm
      echo buckets are ---- ${local.code_bucket} ${local.dataset_bucket}

      sed -i "s/RELEASE_VERSION: '{{ RELEASE_VERSION }}'/RELEASE_VERSION: '${var.pcm_branch}'/g" ~/mozart/ops/opera-pcm/conf/settings.yaml

      if [ "${var.pge_sim_mode}" = false ]; then
        sed -i 's/PGE_SIMULATION_MODE: !!bool true/PGE_SIMULATION_MODE: !!bool false/g' ~/mozart/ops/opera-pcm/conf/settings.yaml
      fi
      sed -i "s/DATASET_BUCKET: '{{ DATASET_BUCKET }}'/DATASET_BUCKET: '${local.dataset_bucket}'/g" ~/mozart/ops/opera-pcm/conf/settings.yaml

      if [ "${var.use_artifactory}" = true ]; then
        fab -f ~/.sds/cluster.py -R mozart,grq,metrics,factotum update_${var.project}_packages
      else
        fab -f ~/.sds/cluster.py -R mozart,grq,metrics,factotum update_${var.project}_packages
      fi
      if [ "${var.grq_aws_es}" = true ] && [ "${var.use_grq_aws_es_private_verdi}" = true ]; then
        fab -f ~/.sds/cluster.py -R mozart update_celery_config
      fi

      fab -f ~/.sds/cluster.py -R grq update_grq_es
      fab -f ~/.sds/cluster.py -R metrics update_metrics_es

      sds -d ship

      cd ~/mozart/pkgs
      sds -d pkg import container-hysds_lightweight-jobs-*.sdspkg.tar
      aws s3 cp hysds-verdi-${var.hysds_release}.tar.gz s3://${local.code_bucket}/ --no-progress
      aws s3 cp docker-registry-2.tar.gz s3://${local.code_bucket}/ --no-progress
      aws s3 cp logstash-7.9.3.tar.gz s3://${local.code_bucket}/ --no-progress
      sds -d reset all -f
      cd ~/mozart/ops/pcm_commons
      pip install --progress-bar off -e .
      cd ~/mozart/ops/opera-pcm
      echo # download dependencies for CLI execution of daac_data_subscriber.py
      pip install '.[subscriber]'
      pip install '.[audit]'

      # comment out on 5-15-24 due to deployment failure
      #pip install '.[cmr_audit]'
      pip install --progress-bar off -e .

      # For daac_data_subscriber utility tool
      mkdir ~/Downloads/
      aws s3 cp  s3://opera-ancillaries/mgrs_tiles/dswx_s1/MGRS_tile_collection_v0.3.sqlite ~/Downloads/
    EOT
    ]
  }

  // Snapshot repositories and lifecycles for GRQ mozart and metrics ES, also set shard max
  // Snapshot schedule is in UTC, 5 AM UTC is 9/10 PM PST, depending on daylight savingss
  provisioner "remote-exec" {
    inline = [<<-EOT
     while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 10; done
      set -ex
      source ~/.bash_profile
      echo // grq
      ~/mozart/bin/snapshot_es_data.py --es-url ${local.grq_es_url} create-repository --repository snapshot-repository --bucket ${var.es_snapshot_bucket} --bucket-path ${var.project}-${var.venue}-${var.counter}/grq --role-arn ${var.es_bucket_role_arn}
      ~/mozart/bin/snapshot_es_data.py --es-url ${local.grq_es_url} create-lifecycle --repository snapshot-repository --policy-id daily-snapshot --snapshot grq-backup --index-pattern grq_*,*_catalog --schedule="0 0 5 * * ?"
      curl -XPUT ${local.grq_es_url}/_cluster/settings -H 'Content-type: application/json' --data-binary $'{"transient":{"cluster.max_shards_per_node": 6000, "search.max_open_scroll_context": 6000}, "persistent":{"cluster.max_shards_per_node": 6000, "search.max_open_scroll_context": 6000}}'

      echo // mozart
      ~/mozart/bin/snapshot_es_data.py --es-url http://${aws_instance.mozart.private_ip}:9200 create-repository --repository snapshot-repository --bucket ${var.es_snapshot_bucket} --bucket-path ${var.project}-${var.venue}-${var.counter}/mozart --role-arn ${var.es_bucket_role_arn}
      ~/mozart/bin/snapshot_es_data.py --es-url http://${aws_instance.mozart.private_ip}:9200 create-lifecycle --repository snapshot-repository --policy-id daily-snapshot --snapshot mozart-backup --index-pattern *_status-*,user_rules-*,job_specs,hysds_ios-*,containers --schedule="0 0 5 * * ?"
      curl -XPUT http://${aws_instance.mozart.private_ip}:9200/_cluster/settings -H 'Content-type: application/json' --data-binary $'{"transient":{"cluster.max_shards_per_node": 6000, "search.max_open_scroll_context": 6000}, "persistent":{"cluster.max_shards_per_node": 6000, "search.max_open_scroll_context": 6000}}'

      echo // metrics
      ~/mozart/bin/snapshot_es_data.py --es-url http://${aws_instance.metrics.private_ip}:9200 create-repository --repository snapshot-repository --bucket ${var.es_snapshot_bucket} --bucket-path ${var.project}-${var.venue}-${var.counter}/metrics --role-arn ${var.es_bucket_role_arn}
      ~/mozart/bin/snapshot_es_data.py --es-url http://${aws_instance.metrics.private_ip}:9200 create-lifecycle --repository snapshot-repository --policy-id daily-snapshot --snapshot metrics-backup --index-pattern logstash-*,sdswatch-*,mozart-logs-*,factotum-logs-*,grq-logs-* --schedule="0 0 5 * * ?"
      curl -XPUT http://${aws_instance.metrics.private_ip}:9200/_cluster/settings -H 'Content-type: application/json' --data-binary $'{"transient":{"cluster.max_shards_per_node": 6000, "search.max_open_scroll_context": 6000}, "persistent":{"cluster.max_shards_per_node": 6000, "search.max_open_scroll_context": 6000}}'

    EOT
    ]
  }
}

resource "null_resource" "bach_and_deploy_pges" {
  depends_on = [
    aws_instance.mozart
  ]

  connection {
    type = "ssh"
    host = aws_instance.mozart.private_ip
    user = "hysdsops"
    private_key = file(var.private_key_file)
  }

  # sync bach-api and bach-ui code. start bach-ui
  provisioner "remote-exec" {
    inline = [<<-EOT
      while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 10; done
      set -ex
      cd ~/mozart/ops
      if [ "${var.use_artifactory}" = true ]; then
        ~/download_artifact.sh -m "${var.artifactory_mirror_url}" -b "${var.artifactory_base_url}" "${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/${var.project}-sds-bach-ui-${var.bach_ui_branch}.tar.gz"
        tar xfz ${var.project}-sds-bach-ui-${var.bach_ui_branch}.tar.gz
        ln -s /export/home/hysdsops/mozart/ops/${var.project}-sds-bach-ui-${var.bach_ui_branch} /export/home/hysdsops/mozart/ops/bach-ui
        rm -rf ${var.project}-sds-bach-ui-${var.bach_ui_branch}.tar.gz
      else
        git clone --quiet --single-branch -b ${var.bach_ui_branch} https://${var.git_auth_key}@${var.bach_ui_repo} bach-ui
      fi

      export PATH=~/conda/bin:$PATH

      cd bach-ui
      ~/conda/bin/npm install --silent --no-progress
      sh create_config_simlink.sh ~/.sds/config ~/mozart/ops/bach-ui
      ~/conda/bin/npm run build --silent
    EOT
    ]
  }

  # deploy PGEs
  provisioner "remote-exec" {
    inline = [<<-EOT
      set -ex
      source ~/.bash_profile
      %{for pge_name, pge_version in var.pge_releases~}
      if [[ \"${pge_version}\" == \"develop\"* ]]; then
          python ~/mozart/ops/opera-pcm/tools/deploy_pges.py \
          --image_names opera_pge-${pge_name} \
          --pge_release ${pge_version} \
          --sds_config ~/.sds/config \
          --processes 4 \
          --force \
          --artifactory_url ${local.pge_artifactory_dev_url}/${pge_name} \
          --username ${var.artifactory_fn_user} \
          --api_key ${var.artifactory_fn_api_key}
      else
          python ~/mozart/ops/opera-pcm/tools/deploy_pges.py \
          --image_names opera_pge-${pge_name} \
          --pge_release ${pge_version} \
          --sds_config ~/.sds/config \
          --processes 4 \
          --force \
          --artifactory_url ${local.pge_artifactory_release_url}/${pge_name} \
          --username ${var.artifactory_fn_user} \
          --api_key ${var.artifactory_fn_api_key}
      fi
      %{endfor~}
      sds -d kibana import -f
      sds -d cloud storage ship_style --bucket ${local.dataset_bucket}
      sds -d cloud storage ship_style --bucket ${local.osl_bucket}
      sds -d cloud storage ship_style --bucket ${local.triage_bucket}
      sds -d cloud storage ship_style --bucket ${local.lts_bucket}
    EOT
    ]
  }
}

# Resource to install PCM and its dependencies, container-nasa-xxx-sds-pcm
resource "null_resource" "install_pcm_and_pges" {
  depends_on = [
    aws_instance.mozart
  ]

  connection {
    type        = "ssh"
    host        = aws_instance.mozart.private_ip
    user        = "hysdsops"
    private_key = file(var.private_key_file)
  }

  provisioner "remote-exec" {
    inline = [<<-EOT
      while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 10; done
      set -ex
      source ~/.bash_profile

      echo build/import opera-pcm
      echo Build container

      if [ "${var.use_artifactory}" = true ]; then
          ~/mozart/ops/${var.project}-pcm/tools/download_artifact.sh -m ${var.artifactory_mirror_url} -b ${var.artifactory_base_url} ${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/hysds_pkgs/container-nasa_${var.project}-sds-pcm-${var.pcm_branch}.sdspkg.tar
        sds pkg import container-nasa_${var.project}-sds-pcm-${var.pcm_branch}.sdspkg.tar
          rm -rf container-nasa_${var.project}-sds-pcm-${var.pcm_branch}.sdspkg.tar
          fab -f ~/.sds/cluster.py -R mozart load_container_in_registry:"container-nasa_${var.project}-sds-pcm:${lower(var.pcm_branch)}"
      else
          sds -d ci add_job -b ${var.pcm_branch} --token https://${var.pcm_repo} s3
          sds -d ci build_job -b ${var.pcm_branch} https://${var.pcm_repo}
          sds -d ci remove_job -b ${var.pcm_branch} https://${var.pcm_repo}
      fi

    EOT
    ]
  }
}

# Resource to install PCM and its dependencies,container-iems-sds_cnm_product_delivery
# Comment out this to override CNM delivery with OPERA PCM repo
#resource "null_resource" "install_pcm_and_pges_iems" {
#  depends_on = [
#    aws_instance.mozart
#  ]
#
#  connection {
#    type        = "ssh"
#    host        = aws_instance.mozart.private_ip
#    user        = "hysdsops"
#    private_key = file(var.private_key_file)
#  }
#
#  provisioner "remote-exec" {
#    inline = [<<-EOT
#      while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 10; done
#      set -ex
#      source ~/.bash_profile
#
#      echo build/import opera-pcm
#      echo Build container
#
#      echo build/import CNM product delivery
#      if [ "${var.use_artifactory}" = true ]; then
#          ~/mozart/ops/${var.project}-pcm/tools/download_artifact.sh -m ${var.artifactory_mirror_url} -b ${var.artifactory_base_url} ${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/hysds_pkgs/container-iems-sds_cnm_product_delivery-${var.product_delivery_branch}.sdspkg.tar
#          sds pkg import container-iems-sds_cnm_product_delivery-${var.product_delivery_branch}.sdspkg.tar
#          rm -rf container-iems-sds_cnm_product_delivery-${var.product_delivery_branch}.sdspkg.tar
#      else
#          sleep 300
#          sds -d ci add_job -b ${var.product_delivery_branch} --token https://${var.product_delivery_repo} s3
#          sds -d ci build_job -b ${var.product_delivery_branch} https://${var.product_delivery_repo}
#          sds -d ci remove_job -b ${var.product_delivery_branch} https://${var.product_delivery_repo}
#      fi
#
#    EOT
#    ]
#  }
#}

resource "null_resource" "setup_trigger_rules" {
  #depends_on = [null_resource.install_pcm_and_pges, null_resource.install_pcm_and_pges_iems]
  depends_on = [null_resource.install_pcm_and_pges]

  connection {
    type        = "ssh"
    host        = aws_instance.mozart.private_ip
    user        = "hysdsops"
    private_key = file(var.private_key_file)
  }

  provisioner "remote-exec" {
    inline = [<<-EOT
      while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 10; done
      set -ex
      source ~/.bash_profile

      echo Set up trigger rules
      sh ~/mozart/ops/${var.project}-pcm/cluster_provisioning/setup_trigger_rules.sh ${aws_instance.mozart.private_ip}

    EOT
    ]
  }
}
