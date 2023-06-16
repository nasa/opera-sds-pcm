############################
# Autoscaling Group related
############################

data "aws_subnet_ids" "private_asg_vpc" {
  vpc_id = var.private_asg_vpc
}
data "aws_subnet_ids" "public_asg_vpc" {
  vpc_id = var.public_asg_vpc
}

data "template_file" "launch_template_user_data" {
  for_each = var.queues
  template = <<-EOT
        #!/bin/bash

        BUNDLE_URL=s3://${local.code_bucket}/${each.key}-${var.project}-${var.venue}-${local.counter}.tbz2
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
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_hlsl30_query.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_hlsl30_query.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f"
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_hlss30_query.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_hlss30_query.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f"
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_hls_download.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_hls_download.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f"
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_slcs1a_query.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_slcs1a_query.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f"
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_slcs1b_query.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_slcs1b_query.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f"
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_slc_download.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_slc_download.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f"
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_batch_query.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_batch_query.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f"
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_pcm_int.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_pcm_int.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f"
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_on_demand.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_on_demand.log",
                    "timezone": "Local"
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_sciflo_L3_DSWx_HLS.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_sciflo_L3_DSWx_HLS.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S"
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_sciflo_L2_CSLC_S1.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_sciflo_L2_CSLC_S1.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S"
                  },
                  {
                    "file_path": "/home/ops/verdi/log/opera-job_worker-hls_data_query.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/opera-job_worker-hls_data_query.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f"
                  },
                  {
                    "file_path": "/home/ops/verdi/log/opera-job_worker-hls_data_download.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/opera-job_worker-hls_data_download.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f"
                  },
                  {
                    "file_path": "/home/ops/verdi/log/opera-job_worker-slc_data_query.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/opera-job_worker-slc_data_query.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f"
                  },
                  {
                    "file_path": "/home/ops/verdi/log/opera-job_worker-slc_data_download.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/opera-job_worker-slc_data_download.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f"
                  },
                  {
                    "file_path": "/home/ops/verdi/log/opera-job_worker-hls_data_ingest.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/opera-job_worker-hls_data_ingest.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f"
                  },
                  {
                    "file_path": "/home/ops/verdi/log/opera-job_worker-sciflo-l3_dswx_hls.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/opera-job_worker-sciflo-l3_dswx_hls.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f"
                  },
                  {
                    "file_path": "/home/ops/verdi/log/opera-job_worker-sciflo-l2_cslc_s1.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/opera-job_worker-sciflo-l2_cslc_s1.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f"
                  },
                  {
                    "file_path": "/home/ops/verdi/log/opera-job_worker-sciflo-l2_rtc_s1.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/opera-job_worker-sciflo-l2_rtc_s1.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f"
                  },
                  {
                    "file_path": "/home/ops/verdi/log/opera-job_worker-send_cnm_notify.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/opera-job_worker-send_cnm_notify.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f"
                  },
                  {
                    "file_path": "/home/ops/verdi/log/opera-job_worker-rcv_cnm_notify.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/opera-job_worker-rcv_cnm_notify.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f"
                  }
                ]
              }
            },
            "force_flush_interval" : 15
          }
        }' > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
        /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
        EOT
}

resource "aws_launch_template" "launch_template" {
  depends_on = [data.template_file.launch_template_user_data]

  for_each               = var.queues
  name                   = "${var.project}-${var.venue}-${local.counter}-${each.key}-launch-template"
  image_id               = var.amis["autoscale"]
  key_name               = local.key_name
  user_data              = base64encode(data.template_file.launch_template_user_data[each.key].rendered)
  vpc_security_group_ids = [lookup(each.value, "use_private_vpc", true) ? var.private_verdi_security_group_id : var.public_verdi_security_group_id]

  tags = { Bravo = "pcm" }
  block_device_mappings {
    device_name = "/dev/sda1"
    ebs {
      volume_size           = lookup(each.value, "root_dev_size")
      delete_on_termination = true
    }
  }

  block_device_mappings {
    device_name = "/dev/sdf"
    ebs {
      volume_size = lookup(each.value, "data_dev_size")
      snapshot_id = data.aws_ebs_snapshot.docker_verdi_registry.id
    }
  }

  iam_instance_profile {
    name = var.asg_use_role ? var.asg_role : ""
  }
  tag_specifications {
    resource_type = "volume"

    tags = {
      Bravo = "pcm"
    }
  }
  #This is very important, as it tells terraform to not mess with tags
  lifecycle {
    ignore_changes = [tags]
  }
}

resource "aws_autoscaling_group" "autoscaling_group" {
  for_each                  = var.queues
  name                      = "${var.project}-${var.venue}-${local.counter}-${each.key}"
  depends_on                = [aws_launch_template.launch_template]
  max_size                  = lookup(each.value, "max_size")
  min_size                  = lookup(each.value, "min_size", 0)
  default_cooldown          = 60
  desired_capacity          = lookup(each.value, "min_size", 0)
  health_check_grace_period = 300
  health_check_type         = "EC2"
  protect_from_scale_in     = false
  vpc_zone_identifier       = lookup(each.value, "use_private_vpc", true) ? data.aws_subnet_ids.private_asg_vpc.ids : data.aws_subnet_ids.public_asg_vpc.ids
  metrics_granularity       = "1Minute"
  enabled_metrics = [
    "GroupMinSize",
    "GroupMaxSize",
    "GroupDesiredCapacity",
    "GroupInServiceInstances",
    "GroupPendingInstances",
    "GroupStandbyInstances",
    "GroupTerminatingInstances",
    "GroupTotalInstances"
  ]
  tags = [
    {
      key                 = "Name"
      value               = "${var.project}-${var.venue}-${local.counter}-${each.key}"
      propagate_at_launch = true
    },
#    {
#      key                 = "Venue"
#      value               = "${var.project}-${var.venue}-${local.counter}"
#      propagate_at_launch = true
#    },
    {
      key                 = "Queue"
      value               = each.key
      propagate_at_launch = true
    },
    {
      key                 = "Bravo"
      value               = "pcm"
      propagate_at_launch = true
    },
  ]
  mixed_instances_policy {
    instances_distribution {
      spot_allocation_strategy                 = "lowest-price"
      spot_instance_pools                      = 3
      on_demand_base_capacity                  = 0
      on_demand_percentage_above_base_capacity = 0
    }

    launch_template {
      launch_template_specification {
        launch_template_name = "${var.project}-${var.venue}-${local.counter}-${each.key}-launch-template"
        version              = "$Latest"
      }

      dynamic "override" {
        for_each = toset(lookup(each.value, "instance_type"))
        content {
          instance_type = override.value
        }
      }
    }
  }
  #This is very important, as it tells terraform to not mess with tags
  lifecycle {
    ignore_changes = [tags]
  }
}

resource "aws_autoscaling_policy" "autoscaling_policy" {
  for_each               = var.queues
  name                   = "${var.project}-${var.venue}-${local.counter}-${each.key}-target-tracking"
  policy_type            = "TargetTrackingScaling"
  autoscaling_group_name = "${var.project}-${var.venue}-${local.counter}-${each.key}"
  depends_on             = [aws_autoscaling_group.autoscaling_group]
  target_tracking_configuration {
    customized_metric_specification {

      metric_dimension {
        name  = "AutoScalingGroupName"
        value = "${var.project}-${var.venue}-${local.counter}-${each.key}"
      }

      metric_dimension {
        name  = "Queue"
        value = each.key
      }
      metric_name = "${lookup(each.value, "total_jobs_metric", false) ? "JobsPerInstance" : "JobsWaitingPerInstance"}-${var.project}-${var.venue}-${local.counter}-${each.key}"
      unit        = "None"
      namespace   = "HySDS"
      statistic   = "Maximum"
    }
   # target_value     = 1.0
	target_value     = lookup(each.value, "total_jobs_metric_target_value", 1.0)
    disable_scale_in = true
  }

}
