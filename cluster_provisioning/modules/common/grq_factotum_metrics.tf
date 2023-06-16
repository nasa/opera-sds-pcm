######################
# metrics
######################

resource "aws_instance" "metrics" {
  ami                  = var.amis["metrics"]
  instance_type        = var.metrics["instance_type"]
  key_name             = local.key_name
  availability_zone    = var.az
  iam_instance_profile = var.pcm_cluster_role["name"]
  private_ip           = var.metrics["private_ip"] != "" ? var.metrics["private_ip"] : null
  user_data            = <<-EOT
              #!/bin/bash

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
    Name  = "${var.project}-${var.venue}-${local.counter}-pcm-${var.metrics["name"]}",
    Bravo = "pcm"
  }
  volume_tags = {
    Bravo = "pcm"
  }

  root_block_device {
    volume_size           = var.metrics["root_dev_size"]
    volume_type           = "gp2"
    delete_on_termination = true
  }

  #This is very important, as it tells terraform to not mess with tags
  lifecycle {
    ignore_changes = [tags, volume_tags]
  }
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [var.cluster_security_group_id]

  connection {
    type        = "ssh"
    host        = aws_instance.metrics.private_ip
    user        = "hysdsops"
    private_key = file(var.private_key_file)
  }

  provisioner "local-exec" {
    command = "echo export METRICS_IP=${aws_instance.metrics.private_ip} > metrics_ip.sh"
  }

  provisioner "file" {
    content     = templatefile("${path.module}/bash_profile.metrics.tmpl", {})
    destination = ".bash_profile"
  }

  provisioner "file" {
    source      = "${path.module}/../../../tools/download_artifact.sh"
    destination = "download_artifact.sh"
  }

  provisioner "remote-exec" {
    inline = [
      "while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 5; done",
      "chmod 755 ~/download_artifact.sh",
      "if [ \"${var.hysds_release}\" != \"develop\" ]; then",
      "  ~/download_artifact.sh -m \"${var.artifactory_mirror_url}\" -b \"${var.artifactory_base_url}\" -k \"${var.artifactory_fn_api_key}\" \"${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/${var.hysds_release}/hysds-conda_env-${var.hysds_release}.tar.gz\"",
      "  mkdir -p ~/conda",
      "  tar xfz hysds-conda_env-${var.hysds_release}.tar.gz -C conda",
      "  export PATH=$HOME/conda/bin:$PATH",
      "  conda-unpack",
      "  rm -rf hysds-conda_env-${var.hysds_release}.tar.gz",
      "  ~/download_artifact.sh -m \"${var.artifactory_mirror_url}\" -b \"${var.artifactory_base_url}\" -k \"${var.artifactory_fn_api_key}\" \"${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/${var.hysds_release}/hysds-metrics_venv-${var.hysds_release}.tar.gz\"",
      "  tar xfz hysds-metrics_venv-${var.hysds_release}.tar.gz",
      "  rm -rf hysds-metrics_venv-${var.hysds_release}.tar.gz",
      "fi"
    ]
  }
}


######################
# grq
######################

resource "aws_instance" "grq" {
  ami                  = var.amis["grq"]
  instance_type        = var.grq["instance_type"]
  key_name             = local.key_name
  availability_zone    = var.az
  iam_instance_profile = var.pcm_cluster_role["name"]
  private_ip           = var.grq["private_ip"] != "" ? var.grq["private_ip"] : null
  user_data            = <<-EOT
              #!/bin/bash
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
    Name  = "${var.project}-${var.venue}-${local.counter}-pcm-${var.grq["name"]}",
    Bravo = "pcm"
  }
  volume_tags = {
    Bravo = "pcm"
  }

  root_block_device {
    volume_size           = var.grq["root_dev_size"]
    volume_type           = "gp2"
    delete_on_termination = true
  }
  #This is very important, as it tells terraform to not mess with tags
  lifecycle {
    ignore_changes = [tags, volume_tags]
  }
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [var.cluster_security_group_id]

  connection {
    type        = "ssh"
    host        = aws_instance.grq.private_ip
    user        = "hysdsops"
    private_key = file(var.private_key_file)
  }


  provisioner "local-exec" {
    command = "echo export GRQ_IP=${aws_instance.grq.private_ip} > grq_ip.sh"
  }

  provisioner "file" {
    content     = templatefile("${path.module}/bash_profile.grq.tmpl", {})
    destination = ".bash_profile"
  }

  provisioner "file" {
    source      = "${path.module}/../../../tools/download_artifact.sh"
    destination = "download_artifact.sh"
  }

  provisioner "remote-exec" {
    inline = [
      "while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 5; done",
      "chmod 755 ~/download_artifact.sh",
      "if [ \"${var.hysds_release}\" != \"develop\" ]; then",
      "  ~/download_artifact.sh -m \"${var.artifactory_mirror_url}\" -b \"${var.artifactory_base_url}\" -k \"${var.artifactory_fn_api_key}\" \"${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/${var.hysds_release}/hysds-conda_env-${var.hysds_release}.tar.gz\"",
      "  mkdir -p ~/conda",
      "  tar xfz hysds-conda_env-${var.hysds_release}.tar.gz -C conda",
      "  export PATH=$HOME/conda/bin:$PATH",
      "  conda-unpack",
      "  rm -rf hysds-conda_env-${var.hysds_release}.tar.gz",
      "  ~/download_artifact.sh -m \"${var.artifactory_mirror_url}\" -b \"${var.artifactory_base_url}\" -k \"${var.artifactory_fn_api_key}\" \"${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/${var.hysds_release}/hysds-grq_venv-${var.hysds_release}.tar.gz\"",
      "  tar xfz hysds-grq_venv-${var.hysds_release}.tar.gz",
      "  rm -rf hysds-grq_venv-${var.hysds_release}.tar.gz",
      "fi",
      "if [ \"${var.use_artifactory}\" = true ]; then",
      "  ~/download_artifact.sh -m \"${var.artifactory_mirror_url}\" -b \"${var.artifactory_base_url}\" \"${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/${var.project}-sds-bach-api-${var.bach_api_branch}.tar.gz\"",
      "  tar xfz ${var.project}-sds-bach-api-${var.bach_api_branch}.tar.gz",
      "  ln -s /export/home/hysdsops/mozart/ops/${var.project}-sds-bach-api-${var.bach_api_branch} /export/home/hysdsops/mozart/ops/${var.project}-sds-bach-api",
      "  rm -rf ${var.project}-sds-bach-api-${var.bach_api_branch}.tar.gz ",
      "else",
      "  git clone --quiet --single-branch -b ${var.bach_api_branch} https://${var.git_auth_key}@${var.bach_api_repo} bach-api",
      "fi"
    ]
  }

}


######################
# factotum
######################

resource "aws_instance" "factotum" {
  ami                  = var.amis["factotum"]
  instance_type        = var.factotum["instance_type"]
  key_name             = local.key_name
  availability_zone    = var.az
  iam_instance_profile = var.pcm_cluster_role["name"]
  private_ip           = var.factotum["private_ip"] != "" ? var.factotum["private_ip"] : null
  user_data            = <<-EOT
              #!/bin/bash

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
    Name  = "${var.project}-${var.venue}-${local.counter}-pcm-${var.factotum["name"]}",
    Bravo = "pcm"
  }
  volume_tags = {
    Bravo = "pcm"
  }
  #This is very important, as it tells terraform to not mess with tags
  lifecycle {
    ignore_changes = [tags, volume_tags]
  }
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [var.cluster_security_group_id]

  root_block_device {
    volume_size           = var.factotum["root_dev_size"]
    volume_type           = "gp2"
    delete_on_termination = true
  }

  ebs_block_device {
    device_name           = var.factotum["data_dev"]
    volume_size           = var.factotum["data_dev_size"]
    volume_type           = "gp2"
    delete_on_termination = true
  }

  connection {
    type        = "ssh"
    host        = aws_instance.factotum.private_ip
    user        = "hysdsops"
    private_key = file(var.private_key_file)
  }

  provisioner "local-exec" {
    command = "echo export FACTOTUM_IP=${aws_instance.factotum.private_ip} > factotum_ip.sh"
  }

  provisioner "file" {
    content     = templatefile("${path.module}/bash_profile.verdi.tmpl", {})
    destination = ".bash_profile"
  }

  provisioner "file" {
    source      = "${path.module}/../../../tools/download_artifact.sh"
    destination = "download_artifact.sh"
  }

  provisioner "remote-exec" {
    inline = [<<-EOT
      while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 5; done
      chmod 755 ~/download_artifact.sh
      if [ "${var.hysds_release}" != "develop" ]; then
        ~/download_artifact.sh -m "${var.artifactory_mirror_url}" -b "${var.artifactory_base_url}" -k "${var.artifactory_fn_api_key}" "${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/${var.hysds_release}/hysds-conda_env-${var.hysds_release}.tar.gz"
        mkdir -p ~/conda
        tar xfz hysds-conda_env-${var.hysds_release}.tar.gz -C conda
        export PATH=$HOME/conda/bin:$PATH
        conda-unpack
        rm -rf hysds-conda_env-${var.hysds_release}.tar.gz
        ~/download_artifact.sh -m "${var.artifactory_mirror_url}" -b "${var.artifactory_base_url}" -k "${var.artifactory_fn_api_key}" "${var.artifactory_base_url}/${var.artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/${var.hysds_release}/hysds-verdi_venv-${var.hysds_release}.tar.gz"
        tar xfz hysds-verdi_venv-${var.hysds_release}.tar.gz
        rm -rf hysds-verdi_venv-${var.hysds_release}.tar.gz
      fi
    EOT
    ]
  }
}