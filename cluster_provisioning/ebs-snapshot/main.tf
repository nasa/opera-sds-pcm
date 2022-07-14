provider "aws" {
  shared_credentials_file = var.shared_credentials_file
  region                  = var.region
  profile                 = var.profile
}


locals {
  key_name = var.keypair_name != "" ? var.keypair_name : split(".", basename(var.private_key_file))[0]
}

resource "aws_instance" "verdi" {
  depends_on           = [aws_ebs_volume.verdi_docker]
  ami                  = var.verdi["ami"]
  instance_type        = var.verdi["instance_type"]
  key_name             = local.key_name
  availability_zone    = var.az
  iam_instance_profile = var.pcm_verdi_role["name"]
  private_ip           = var.verdi["private_ip"] != "" ? var.verdi["private_ip"] : null
  tags = {
    Name  = "${var.project}-${var.venue}-pcm-${var.verdi["name"]}-standalone",
    Bravo = "pcm"
  }
  volume_tags = {
    Bravo = "pcm"
  }
  #This is very important, as it tells terraform to not mess with tags
  lifecycle {
    ignore_changes = [tags]
  }
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [var.verdi_security_group_id]

  ebs_block_device {
    device_name           = var.verdi["device_name"]
    volume_size           = var.verdi["device_size"]
    delete_on_termination = true
  }
}

# Create EBS volume
resource "aws_ebs_volume" "verdi_docker" {
  availability_zone = var.az
  size              = var.verdi["docker_device_size"]
  tags = {
    Name  = "${var.project}-${var.venue}-pcm-verdi-docker-ebs-volume",
    Bravo = "pcm"
  }
}

# Attach EBS Volume
resource "aws_volume_attachment" "volume_attachment" {
  device_name = var.verdi["docker_device_name"]
  volume_id   = aws_ebs_volume.verdi_docker.id
  instance_id = aws_instance.verdi.id

  connection {
    type        = "ssh"
    host        = aws_instance.verdi.private_ip
    user        = "ops"
    private_key = file(var.private_key_file)
  }

  provisioner "file" {
    content     = templatefile("${path.module}/bash_profile.verdi.tmpl", {})
    destination = ".bash_profile"
  }

  provisioner "file" {
    content     = templatefile("${path.module}/supervisord.conf.verdi.standalone", {})
    destination = "$${HOME}/verdi/etc/supervisord.conf"
  }

  provisioner "file" {
    content     = templatefile("${path.module}/docker-compose.yml.verdi.standalone", {})
    destination = "$${HOME}/verdi/ops/hysds-dockerfiles/verdi/docker-compose.yml"
  }

  provisioner "remote-exec" {
    inline = [
      "set -ex",
      "source ~/verdi/bin/activate",
      "if [ \"${var.verdi_release}\" = develop ]; then",
      " echo ${var.docker_pwd} | docker login --username ${var.docker_user} --password-stdin",
      " docker pull logstash:${var.logstash_release}",
      " docker pull registry:${var.registry_release}",
      " docker pull hysds/verdi:${var.verdi_release}",
      "else",
      "  wget https://cae-artifactory.jpl.nasa.gov/artifactory/${var.artifactory_repo}/gov/nasa/jpl/iems/sds/pcm/${var.verdi_release}/hysds-mozart_venv-${var.verdi_release}.tar.gz!/mozart/pkgs/hysds-verdi-${var.verdi_release}.tar.gz",
      "  docker load < hysds-verdi-${var.verdi_release}.tar.gz",
      "  rm hysds-verdi-${var.verdi_release}.tar.gz",
      "  curl -O \"https://cae-artifactory.jpl.nasa.gov/artifactory/${var.artifactory_repo}/gov/nasa/jpl/iems/sds/pcm/logstash/${var.logstash_release}/logstash-${var.logstash_release}.tar.gz\"",
      "  docker load < logstash-${var.logstash_release}.tar.gz",
      "  rm logstash-${var.logstash_release}.tar.gz",
      "  curl -O \"https://cae-artifactory.jpl.nasa.gov/artifactory/${var.artifactory_repo}/gov/nasa/jpl/iems/sds/pcm/docker-registry/${var.registry_release}/docker-registry-${var.registry_release}.tar.gz\"",
      "  docker load < docker-registry-${var.registry_release}.tar.gz",
      "  rm docker-registry-${var.registry_release}.tar.gz",
      "fi",
#      "  curl -O \"https://artifactory-fn.jpl.nasa.gov:443/artifactory/${var.artifactory_repo}/gov/nasa/jpl/opera/sds/pcm/pge_snapshots/${var.pge_snapshots_date}/opera_pge-dswx_hls-${var.pge_release}.tar.gz\"",

      "  curl -O \"https://artifactory-fn.jpl.nasa.gov:443/artifactory/${var.artifactory_repo}/gov/nasa/jpl/opera/sds/pge/dswx_hls/opera_pge-dswx_hls-${var.pge_release}.tar.gz\"",
      "  --header 'Authorization: Basic ${base64encode(format("%s:%s", var.artifactory_fn_user, var.artifactory_fn_api_key))}'",
      "  docker load -i opera_pge-dswx_hls-${var.pge_release}.tar.gz",
# TODO chrisjrd: extract variables
#      "  curl -O \"https://artifactory-fn.jpl.nasa.gov/artifactory/general/gov/nasa/jpl/opera/sds/pge/opera_pge-dswx_hls-1.0.0-er.2.0.tar.gz\" \\",
#      "  --header 'Authorization: Basic ${base64encode(format("%s:%s", var.artifactory_fn_user, var.artifactory_fn_api_key))}'",
#      "  docker load -i opera_pge-dswx_hls-1.0.0-er.2.0.tar.gz",

      "docker tag hysds/verdi:${var.verdi_release} hysds/verdi:latest",
      "/etc/systemd/system/start-verdi.d/start-verdi.sh",
      "docker stop $(docker ps -a -q)",
      "docker rm $(docker ps -a -q)",
      "echo 'SUCCESS: EBS Snapshot was created' > /tmp/result.txt"
    ]
  }

  provisioner "remote-exec" {
    inline = [
      "set -ex",
      "source ~/.bash_profile",
      "SUCCESS=$(grep -c ^SUCCESS /tmp/result.txt)",
      "if [[ \"$${SUCCESS}\" -eq 0 ]]; then exit 1; fi",
    ]
  }

  provisioner "local-exec" {
    command = "scp -o StrictHostKeyChecking=no -q -i ${var.private_key_file} ops@${aws_instance.verdi.private_ip}:/tmp/result.txt ."
  }
}

resource "aws_ebs_snapshot" "verdi_docker_snapshot" {
  depends_on  = [aws_volume_attachment.volume_attachment]
  volume_id   = aws_ebs_volume.verdi_docker.id
  description = "EBS Volume Snapshot containing Docker ${var.verdi_release}, Registry ${var.registry_release}, and Logstash ${var.logstash_release}"
  tags = {
    Name     = "${var.project}-${var.venue}-pcm-verdi-docker-ebs-volume-snapshot",
    Bravo    = "pcm"
    Verdi    = var.verdi_release
    Registry = var.registry_release
    Logstash = var.logstash_release
    l0a      = var.pge_release
  }
  #This is very important, as it tells terraform to not mess with tags
  lifecycle {
    ignore_changes  = [tags]
    prevent_destroy = true
  }
}
