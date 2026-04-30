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
  ami                  = data.aws_ami.mozart_ami.id
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
                  "force_flush_interval": 15
                },
                "metrics": {
                  "append_dimensions": {
                    "ImageId": "$${aws:ImageId}",
                    "InstanceId": "$${aws:InstanceId}",
                    "InstanceType": "$${aws:InstanceType}"
                  },
                  "metrics_collected": {
                    "cpu": {
                      "measurement": [
                        "cpu_usage_iowait",
                        "cpu_usage_user",
                        "cpu_usage_system"
                      ],
                      "metrics_collection_interval": 60,
                      "resources": [
                        "*"
                      ],
                      "totalcpu": true
                    },
                    "disk": {
                      "measurement": [
                        "used_percent"
                      ],
                      "metrics_collection_interval": 60,
                      "resources": [
                        "*"
                      ]
                    },
                    "mem": {
                      "measurement": [
                        "mem_used_percent"
                      ],
                      "metrics_collection_interval": 60
                    }
                  }
                }
              }' > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
              /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
              EOT
  tags = {
    Name  = "${var.project}-${var.venue}-${local.counter}-pcm-${var.mozart["name"]}",
    ESIdentifier = local.es_identifier,
    Bravo = "pcm",
    DNS = "True"
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

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "optional"
    http_put_response_hop_limit = 3
    instance_metadata_tags      = "enabled"
  }

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

      # DIT cert paths consumed by celeryconfig.py.tmpl.{private_verdi,asg}
      # broker_use_ssl block. Static paths from the v6.0+ AMI bake.
      echo CA_BUNDLE_CERT: /etc/pki/tls/certs/ca-bundle.crt >> ~/.sds/config
      echo LOCALHOST_CERT: /etc/pki/tls/certs/localhost.crt >> ~/.sds/config
      echo LOCALHOST_KEYFILE: /etc/pki/tls/private/localhost.key >> ~/.sds/config
      echo CIPHERS: DHE-RSA-AES128-GCM-SHA256 >> ~/.sds/config
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


      echo MOZART_ES_ENGINE: ${tonumber(substr(local.ami_versions["mozart"], 1, 1)) >= 5 ? "opensearch" : "elasticsearch"} >> ~/.sds/config
      echo MOZART_ES_PVT_IP: ${local.es_cluster_mode ? "" : aws_instance.mozart.private_ip} >> ~/.sds/config
      if [ "${local.es_cluster_mode}" = true ]; then
        echo '    - ${aws_instance.mozart.private_ip}' >> ~/.sds/config
        echo '    - ${aws_instance.grq.private_ip}' >> ~/.sds/config
        echo '    - ${aws_instance.metrics.private_ip}' >> ~/.sds/config
      fi
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

      echo METRICS_ES_ENGINE: ${tonumber(substr(local.ami_versions["metrics"], 1, 1)) >= 5 ? "opensearch" : "elasticsearch"} >> ~/.sds/config
      echo METRICS_ES_PVT_IP: ${local.es_cluster_mode ? "" : aws_instance.metrics.private_ip} >> ~/.sds/config
      if [ "${local.es_cluster_mode}" = true ]; then
        echo '    - ${aws_instance.metrics.private_ip}' >> ~/.sds/config
        echo '    - ${aws_instance.mozart.private_ip}' >> ~/.sds/config
        echo '    - ${aws_instance.grq.private_ip}' >> ~/.sds/config
      fi

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
      echo GRQ_ES_ENGINE: ${tonumber(substr(local.ami_versions["grq"], 1, 1)) >= 5 ? "opensearch" : "elasticsearch"} >> ~/.sds/config
      echo GRQ_ES_PVT_IP: ${local.es_cluster_mode ? "" : aws_instance.grq.private_ip} >> ~/.sds/config
      if [ "${local.es_cluster_mode}" = true ]; then
        echo '    - ${aws_instance.grq.private_ip}' >> ~/.sds/config
        echo '    - ${aws_instance.mozart.private_ip}' >> ~/.sds/config
        echo '    - ${aws_instance.metrics.private_ip}' >> ~/.sds/config
      fi

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

      echo ES_CLUSTER_MODE: ${local.es_cluster_mode} >> ~/.sds/config
      echo >> ~/.sds/config
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
      echo HOST_VERDI_HOME: "$HOME" >> ~/.sds/config
      echo VERDI_HOME: "root" >> ~/.sds/config
      echo VERDI_SHELL: "/bin/bash" >> ~/.sds/config
      echo VENUE: ${var.project}-${var.venue}-${local.counter} >> ~/.sds/config
      echo >> ~/.sds/config

      echo ASG: >> ~/.sds/config
      echo '  SSM_ARN: ${local.verdi_ssm_arn}' >> ~/.sds/config
      echo '  AMI: ${data.aws_ami.autoscale_ami.id}' >> ~/.sds/config
      echo '  KEYPAIR: ${local.key_name}' >> ~/.sds/config
      echo '  USE_ROLE: ${var.asg_use_role}' >> ~/.sds/config
      echo '  ROLE: ${var.asg_role}' >> ~/.sds/config
      echo >> ~/.sds/config
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

      #echo GRQ_ES_PUB_IP: ${var.grq_aws_es ? var.grq_aws_es_host : aws_instance.grq.private_ip} >> ~/.sds/config
      echo MOZART_ES_CLUSTER: ${local.es_cluster_mode ? "common_cluster" : "resource_cluster"} >> ~/.sds/config
      echo METRICS_ES_CLUSTER: ${local.es_cluster_mode ? "common_cluster" : "metrics_cluster"} >> ~/.sds/config
      echo DATASET_QUERY_INDEX: grq >> ~/.sds/config
      echo USER_RULES_DATASET_INDEX: user_rules >> ~/.sds/config
      echo EXTRACTOR_HOME: /home/ops/verdi/ops/${var.project}-pcm/extractor >> ~/.sds/config
      echo CONTAINER_ENGINE: docker >> ~/.sds/config
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

      # Sync ~/.netrc-os across cluster nodes so all three share the same Opensearch
      # hysdsops password for app-level auth.
      #
      # The OL8 AMI's project-setup-ol8.sh writes ~/.netrc-os only on grq (the first
      # node up per the depends_on chain) -- on mozart and metrics the clustering
      # block apparently doesn't run to completion, so they never get the file
      # locally. Pattern from swot-pcm ec2_mozart.tf:124-140: fetch from grq, push
      # to metrics. mozart keeps a local copy (overwritten with grq's) so its apps
      # auth correctly.
      #
      # Skipped in standalone mode -- in es_cluster_mode=false the AMI doesn't enter
      # the OpenSearch clustering block at all and ~/.netrc-os doesn't exist anywhere.
      if [ "${local.es_cluster_mode}" = true ]; then
        scp -o StrictHostKeyChecking=no -q -i ~/.ssh/${basename(var.private_key_file)} hysdsops@${aws_instance.grq.private_ip}:~/.netrc-os ~/.netrc-os
        scp -o StrictHostKeyChecking=no -q -i ~/.ssh/${basename(var.private_key_file)} ~/.netrc-os hysdsops@${aws_instance.metrics.private_ip}:~/.netrc-os

        # Now that ~/.netrc-os is on mozart, extract OS_USER/OS_PASSWORD into
        # ~/.sds/config so celeryconfig.py.tmpl.{private_verdi,asg} can render
        # them into Verdi worker celery configs (broker auth + OS http_auth).
        # Format of ~/.netrc-os: "default login <user> password <pwd>"
        echo OS_USER: $(awk 'NR==1{print $3; exit}' ~/.netrc-os) >> ~/.sds/config
        echo OS_PASSWORD: $(awk 'NR==1{print $5; exit}' ~/.netrc-os) >> ~/.sds/config
        echo >> ~/.sds/config
      fi
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
      echo installing gdal for manual execution of daac_data_subscriber.py ;
      conda install -y -c conda-forge conda gdal poppler --yes --quiet ;

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
        conda install -y -c conda-forge conda gdal poppler --yes --quiet

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
  provisioner "remote-exec" {
    inline = [<<-EOT
      while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 10; done
      set -x
      if [ "${local.use_mozart_es}" = true ]; then
        sudo systemctl stop ${tonumber(substr(local.ami_versions["mozart"], 1, 1)) >= 5 ? "opensearch" : "elasticsearch"}
        sudo systemctl disable ${tonumber(substr(local.ami_versions["mozart"], 1, 1)) >= 5 ? "opensearch" : "elasticsearch"}
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

      # pip 21.3+ defaults to strict editable mode (creates an __editable__.<pkg>.pth
      # that registers a finder for the package only). This hides bare .py siblings
      # of the package from sys.path -- including hysds-3.1.1/celeryconfig.py, which
      # `sds -d update` itself triggers via its internal fab task chain
      # (ensure_venv -> mozartd_stop -> rm_rf -> send_celeryconf -> install_base_es_template
      # -> celery's _smart_import("celeryconfig")). The AMI was baked with strict-mode
      # pip, so reinstall hysds + hysds_commons in compat mode BEFORE sds -d update
      # so its internal install_base_es_template doesn't crash with
      # ModuleNotFoundError: No module named 'celeryconfig'.
      reinstall_hysds_compat() {
        for pkg in hysds hysds_commons; do
          if [ -d ~/mozart/ops/$pkg ]; then
            (cd ~/mozart/ops/$pkg && pip install -e . --config-settings editable_mode=compat)
          fi
        done
      }
      reinstall_hysds_compat

      # Pre-stage the OpenSearch ISM policy file in ~/.sds/files/ so that the
      # sdscli internal `install_es_policy` fab task (invoked by `sds -d update`
      # under "Setting ES Index Lifecycle Policy") can find it. Historically
      # this was done by OPERA's update_ilm_policy_mozart fab task, but that
      # task was removed in commit fac65cc9 (replaced with a direct curl PUT
      # below) -- without it, install_es_policy fails with
      # jinja2.exceptions.TemplateNotFound. The .tmpl has no Jinja variables
      # so we just cp it as the rendered file.
      mkdir -p ~/.sds/files
      cp ~/mozart/ops/opera-pcm/conf/sds/files/opensearch_ism_policy_mozart.json.tmpl \
         ~/.sds/files/opensearch_ism_policy_mozart.json

      # Wait for OpenSearch security plugin to finish bootstrapping on EACH cluster
      # node. project-setup-ol8.sh runs securityadmin.sh independently on mozart,
      # grq, and metrics; if `sds -d update`'s install_base_es_template targets a
      # node whose local OS daemon hasn't yet bootstrapped its .opendistro_security
      # index, the PUT returns:
      #   TransportError(503, 'OpenSearch Security not initialized.')
      # The PUTs hit each node's *local* https://<ip>:9200 endpoint (not
      # local.grq_es_url which is HTTP and grq-only), so wait on each role's
      # HTTPS health endpoint individually. Up to 10 minutes per node.
      wait_for_os_security() {
        local url="$1"
        for i in {1..60}; do
          code=$(curl -k --netrc-file ~/.netrc-os -sS -o /dev/null -w '%%{http_code}' "$url" 2>/dev/null || echo 000)
          if [ "$code" = "200" ]; then
            echo "OpenSearch security initialized at $url (200)"
            return 0
          fi
          echo "Waiting for OpenSearch security at $url (attempt $i/60, last code=$code)..."
          sleep 10
        done
        echo "WARN: OpenSearch security at $url did not return 200 within 10 min; proceeding anyway"
      }
      if [ "${local.es_cluster_mode}" = true ]; then
        wait_for_os_security "https://${aws_instance.mozart.private_ip}:9200/"
        wait_for_os_security "https://${aws_instance.grq.private_ip}:9200/"
        wait_for_os_security "https://${aws_instance.metrics.private_ip}:9200/"
      else
        wait_for_os_security "https://${aws_instance.mozart.private_ip}:9200/"
      fi

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

      # Install mozart ISM policy via direct REST PUT against OpenSearch instead of
      # the historical `fab -R mozart update_ilm_policy_mozart` task. NISAR pattern,
      # see nisar-pcm/cluster_provisioning/modules/common/main.tf:1888-1896.
      #
      # Why: under hysds v3.1.1 + py3.12 + pip 21.3+ strict editable mode, any fab
      # task on mozart that runs through hysds_commons + celery triggers
      # celery._smart_import("celeryconfig") which can't find the bare module
      # (celeryconfig.py is a sibling of the hysds package, not inside it).
      # Result: ModuleNotFoundError: No module named 'celeryconfig'.
      #
      # Doing the PUT directly with curl --netrc-file ~/.netrc-os bypasses Python
      # entirely. The .tmpl source has no Jinja variables so we PUT it as-is.
      if [ "${local.es_cluster_mode}" = true ]; then
        MOZART_OS_URL="${local.grq_es_url}"
      else
        MOZART_OS_URL="https://${aws_instance.mozart.private_ip}:9200"
      fi
      curl -k --netrc-file ~/.netrc-os \
        -XPUT "$MOZART_OS_URL/_plugins/_ism/policies/ilm_policy_mozart?pretty" \
        -H 'Content-Type: application/json' \
        -d@$HOME/mozart/ops/opera-pcm/conf/sds/files/opensearch_ism_policy_mozart.json.tmpl

      # Safety net: sds -d update may have reinstalled hysds in default (strict)
      # mode -- redo the compat reinstall so subsequent fab tasks (update_grq_es,
      # update_metrics_es, etc.) don't crash on the same celeryconfig import.
      reinstall_hysds_compat
      cd ~

      if [ "${var.use_artifactory}" = true ]; then
         cp -pr /export/home/hysdsops/mozart/ops/opera-sds-pcm-${var.pcm_branch} ~/verdi/ops/opera-pcm
      else
         cp -pr ~/mozart/ops/opera-pcm ~/verdi/ops/opera-pcm
      fi

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
      aws s3 cp logstash-oss-7.16.3.tar.gz s3://${local.code_bucket}/ --no-progress
      sds -d reset all -f
      cd ~/mozart/ops/pcm_commons
      pip install --progress-bar off -e .
      cd ~/mozart/ops/opera-pcm
      echo # download dependencies for CLI execution of daac_data_subscriber.py
      pip install '.[subscriber]'
      pip install '.[audit]'
      pip install '.[disp_s1_status]'

      # comment out on 5-15-24 due to deployment failure
      #pip install '.[cmr_audit]'
      pip install --progress-bar off -e .

      # For daac_data_subscriber utility tool
      mkdir ~/Downloads/
      aws s3 cp  s3://opera-ancillaries/mgrs_tiles/dswx_s1/MGRS_tile_collection_v0.3.sqlite ~/Downloads/

      # For DISP-S1 perpendicular fix
      cd product_update/disp_s1_r4_bperp/docker 
      sh build_and_deploy.sh ${local.code_bucket}
    EOT
    ]
  }

  # deploy PGEs
  provisioner "remote-exec" {
    inline = [<<-EOT
      set -ex
      source ~/.bash_profile
      %{for pge_name, pge_version in var.pge_releases~}
      cat > /tmp/deploy_${pge_name}.sh << 'SCRIPT'
      #!/bin/bash
      source ~/.bash_profile
      if [[ "${pge_version}" == "develop"* ]]; then
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
      SCRIPT
      chmod +x /tmp/deploy_${pge_name}.sh
      %{endfor~}

      # Run all in parallel with xargs
      ls /tmp/deploy_*.sh | xargs --max-procs=2 -I {} bash {}

      # Cleanup
      rm -f /tmp/deploy_*.sh

      sds -d kibana import -f
      sds -d cloud storage ship_style --bucket ${local.dataset_bucket}
      sds -d cloud storage ship_style --bucket ${local.osl_bucket}
      sds -d cloud storage ship_style --bucket ${local.triage_bucket}
      sds -d cloud storage ship_style --bucket ${local.lts_bucket}
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

      export MOZART_ES_ENGINE=`grep "MOZART_ES_ENGINE" ~/.sds/config | sed 's/MOZART_ES_ENGINE: //g'`
      export METRICS_ES_ENGINE=`grep "METRICS_ES_ENGINE" ~/.sds/config | sed 's/METRICS_ES_ENGINE: //g'`
      export GRQ_ES_ENGINE=`grep "GRQ_ES_ENGINE" ~/.sds/config | sed 's/GRQ_ES_ENGINE: //g'`

      if [ "${local.es_cluster_mode}" = false ]; then
        echo // grq
        curl -k --netrc-file ~/.netrc-os -XPUT ${local.grq_es_url}/_cluster/settings -H 'Content-type: application/json' --data-binary $'{"transient":{"cluster.max_shards_per_node": 6000, "search.max_open_scroll_context": 6000}, "persistent":{"cluster.max_shards_per_node": 6000, "search.max_open_scroll_context": 6000}}'
        ~/mozart/bin/snapshot_es_data.py --engine $GRQ_ES_ENGINE --es-url ${local.grq_es_url} create-repository --repository grq-snapshot-repo --bucket ${var.es_snapshot_bucket} --bucket-path ${var.project}-${var.venue}-${var.counter}/grq --role-arn ${var.es_bucket_role_arn}
        ~/mozart/bin/snapshot_es_data.py --engine $GRQ_ES_ENGINE --es-url ${local.grq_es_url} create-lifecycle --repository grq-snapshot-repo --policy-id daily-snapshot --snapshot grq-backup --index-pattern grq_*,*_catalog --schedule="0 0 5 * * ?"

        echo // mozart
        curl -k --netrc-file ~/.netrc-os -XPUT https://${aws_instance.mozart.private_ip}:9200/_cluster/settings -H 'Content-type: application/json' --data-binary $'{"transient":{"cluster.max_shards_per_node": 6000, "search.max_open_scroll_context": 6000}, "persistent":{"cluster.max_shards_per_node": 6000, "search.max_open_scroll_context": 6000}}'
        ~/mozart/bin/snapshot_es_data.py --engine $MOZART_ES_ENGINE --es-url https://${aws_instance.mozart.private_ip}:9200 create-repository --repository mozart-snapshot-repo --bucket ${var.es_snapshot_bucket} --bucket-path ${var.project}-${var.venue}-${var.counter}/mozart --role-arn ${var.es_bucket_role_arn}
        ~/mozart/bin/snapshot_es_data.py --engine $MOZART_ES_ENGINE --es-url https://${aws_instance.mozart.private_ip}:9200 create-lifecycle --repository mozart-snapshot-repo --policy-id daily-snapshot --snapshot mozart-backup --index-pattern *_status-*,user_rules-*,job_specs,hysds_ios-*,containers --schedule="0 0 5 * * ?"

        echo // metrics
        curl -k --netrc-file ~/.netrc-os -XPUT https://${aws_instance.metrics.private_ip}:9200/_cluster/settings -H 'Content-type: application/json' --data-binary $'{"transient":{"cluster.max_shards_per_node": 6000, "search.max_open_scroll_context": 6000}, "persistent":{"cluster.max_shards_per_node": 6000, "search.max_open_scroll_context": 6000}}'
        ~/mozart/bin/snapshot_es_data.py --engine $METRICS_ES_ENGINE --es-url https://${aws_instance.metrics.private_ip}:9200 create-repository --repository metrics-snapshot-repo --bucket ${var.es_snapshot_bucket} --bucket-path ${var.project}-${var.venue}-${var.counter}/metrics --role-arn ${var.es_bucket_role_arn}
        ~/mozart/bin/snapshot_es_data.py --engine $METRICS_ES_ENGINE --es-url https://${aws_instance.metrics.private_ip}:9200 create-lifecycle --repository metrics-snapshot-repo --policy-id daily-snapshot --snapshot metrics-backup --index-pattern logstash-*,sdswatch-*,mozart-logs-*,factotum-logs-*,grq-logs-* --schedule="0 0 5 * * ?"
      else
        ~/mozart/bin/snapshot_es_data.py --engine $GRQ_ES_ENGINE --es-url ${local.grq_es_url} create-repository --repository snapshot-repo --bucket ${var.es_snapshot_bucket} --bucket-path ${var.project}-${var.venue}-${var.counter}/cluster --role-arn ${var.es_bucket_role_arn}
        ~/mozart/bin/snapshot_es_data.py --engine $GRQ_ES_ENGINE --es-url ${local.grq_es_url} create-lifecycle --repository snapshot-repo --policy-id hourly-snapshot --snapshot common-cluster-backup --index-pattern grq_*,*_catalog,*_status-*,user_rules-*,job_specs,hysds_ios-*,containers,logstash-*,sdswatch-*,mozart-logs-*,factotum-logs-*,grq-logs-*
      fi
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

resource "null_resource" "setup_cron_mozart" {
  depends_on = [aws_instance.mozart]

  connection {
    type        = "ssh"
    host        = aws_instance.mozart.private_ip
    user        = "hysdsops"
    private_key = file(var.private_key_file)
  }

  provisioner "file" {
    source      = "${path.module}/../../../ecmwf-api-client/run_ecmwf_merger_daily.sh"
    destination = "run_ecmwf_merger_daily.sh"
  }

  # Set up crontab for updating DISP-S1 historical processing status
  provisioner "remote-exec" {
    inline = [<<-EOT
      source ~/.bash_profile
      set -ex
      if [ "${var.disp_s1_hist_status}" = true ]; then
        crontab ~/mozart/ops/opera-pcm/conf/sds/files/mozart/cron/hysdsops
      else
        chmod +x ~/run_ecmwf_merger_daily.sh
        mkdir -p .local/bin/cron
        mv ~/run_ecmwf_merger_daily.sh ~/.local/bin/cron/

        crontab ~/mozart/ops/opera-pcm/conf/sds/files/mozart/cron/ecmwf_merger
      fi
    EOT
    ]
  }
}

resource "local_file" "smoke_test_inputs" {
  depends_on      = [aws_instance.mozart]
  filename        = "${path.module}/smoke_test_inputs.config"
  file_permission = "0644"
  content         = <<EOF
project=${var.project}
environment=${var.environment}
venue=${var.venue}
counter=${var.counter}
artifactory=${var.use_artifactory}
artifactory_base_url=${var.artifactory_base_url}
artifactory_repo=${var.artifactory_repo}
artifactorty_mirror_url=${var.artifactory_mirror_url}
pcm_repo=${var.pcm_repo}
pcm_branch=${var.pcm_branch}
product_delivery_repo=${var.product_delivery_repo}
product_delivery_branch=${var.product_delivery_branch}
mozart_private_ip=${aws_instance.mozart.private_ip}
factotum_private_ip=${aws_instance.factotum.private_ip}
isl_bucket=${var.isl_bucket}
dataset_bucket=${var.dataset_bucket}
use_daac_cnm_r=${var.use_daac_cnm_r}
po_daac_delivery_proxy=${var.po_daac_delivery_proxy}
crid=${var.crid}
cluster_type=${var.cluster_type}
  EOF
}

resource "null_resource" "copy_smoke_test_inputs" {
  depends_on = [
    aws_instance.mozart, local_file.smoke_test_inputs
  ]

  triggers = {
    always_run = timestamp()
  }

  connection {
    type        = "ssh"
    host        = aws_instance.mozart.private_ip
    user        = "hysdsops"
    private_key = file(var.private_key_file)
  }

  provisioner "file" {
    source      = "${path.module}/smoke_test_inputs.config"
    destination = "mozart/ops/opera-pcm/cluster_provisioning/smoke_test_inputs.config"
  }
}
