#####################################
# Control-plane CPU architecture
#####################################
# The architecture of each control-plane host is derived from its instance
# type, so a venue moves between x86_64 and Graviton (arm64) by changing the
# instance_type values alone. The derived architecture selects the AMI (see the
# "architecture" filters on the aws_ami data sources) and the HySDS bundles
# (the "-arm64" suffixed tarballs on Graviton).
#
# x86 instance types report ["i386", "x86_64"] or ["x86_64"]; Graviton types
# report ["arm64"]. arm64 is used only when it is the sole supported
# architecture.
data "aws_ec2_instance_type" "mozart" {
  instance_type = var.mozart["instance_type"]
}

data "aws_ec2_instance_type" "metrics" {
  instance_type = var.metrics["instance_type"]
}

data "aws_ec2_instance_type" "grq" {
  instance_type = var.grq["instance_type"]
}

data "aws_ec2_instance_type" "factotum" {
  instance_type = var.factotum["instance_type"]
}

locals {
  mozart_supported_archs   = data.aws_ec2_instance_type.mozart.supported_architectures
  metrics_supported_archs  = data.aws_ec2_instance_type.metrics.supported_architectures
  grq_supported_archs      = data.aws_ec2_instance_type.grq.supported_architectures
  factotum_supported_archs = data.aws_ec2_instance_type.factotum.supported_architectures

  mozart_arch   = contains(local.mozart_supported_archs, "arm64") && !contains(local.mozart_supported_archs, "x86_64") ? "arm64" : "x86_64"
  metrics_arch  = contains(local.metrics_supported_archs, "arm64") && !contains(local.metrics_supported_archs, "x86_64") ? "arm64" : "x86_64"
  grq_arch      = contains(local.grq_supported_archs, "arm64") && !contains(local.grq_supported_archs, "x86_64") ? "arm64" : "x86_64"
  factotum_arch = contains(local.factotum_supported_archs, "arm64") && !contains(local.factotum_supported_archs, "x86_64") ? "arm64" : "x86_64"

  # suffix of the HySDS bundle tarballs: "-arm64" on Graviton, none on x86_64
  mozart_arch_suffix   = local.mozart_arch == "arm64" ? "-arm64" : ""
  metrics_arch_suffix  = local.metrics_arch == "arm64" ? "-arm64" : ""
  grq_arch_suffix      = local.grq_arch == "arm64" ? "-arm64" : ""
  factotum_arch_suffix = local.factotum_arch == "arm64" ? "-arm64" : ""
}

# mozart, grq and metrics form a single OpenSearch cluster, and the develop
# install path builds one conda env on mozart and copies it to the other three
# hosts, so all four control-plane hosts must share one architecture. Worker
# pools are independent of this and may run either architecture.
resource "null_resource" "validate_cluster_architecture_consistency" {
  lifecycle {
    precondition {
      condition = alltrue([
        local.metrics_arch == local.mozart_arch,
        local.grq_arch == local.mozart_arch,
        local.factotum_arch == local.mozart_arch,
      ])
      error_message = "All control-plane hosts must use the same CPU architecture. Detected mozart=${local.mozart_arch} (${var.mozart["instance_type"]}), metrics=${local.metrics_arch} (${var.metrics["instance_type"]}), grq=${local.grq_arch} (${var.grq["instance_type"]}), factotum=${local.factotum_arch} (${var.factotum["instance_type"]}). Use all x86_64 (e.g. r6i, r7i) or all Graviton (e.g. r7g, m7g) instance types."
    }
  }
}
