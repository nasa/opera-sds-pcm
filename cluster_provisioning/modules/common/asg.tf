############################
# Autoscaling Group related
############################

data "aws_subnet_ids" "private_asg_vpc" {
  vpc_id = var.private_asg_vpc
}
data "aws_subnet_ids" "public_asg_vpc" {
  vpc_id = var.public_asg_vpc
}

resource "aws_launch_template" "launch_template" {
  for_each               = var.queues
  name                   = "${var.project}-${var.venue}-${local.counter}-${each.key}-launch-template"
  image_id               = var.amis["autoscale"]
  key_name               = local.key_name
  user_data              = base64encode(templatefile("${path.module}/launch_template_user_data.sh.tmpl", {
    code_bucket = local.code_bucket
    each_key = each.key
    var_project = var.project
    var_venue = var.venue
    local_counter = local.counter
    var_environment = var.environment
    run_log_group = length(split("-", lower(each.key))) == 4 ? split("-", lower(each.key))[3] : split("-", lower(each.key))[2]
    log_file_name = lookup(each.value, "log_file_name", "run_job")
  }))
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
      spot_allocation_strategy                 = "price-capacity-optimized"
      spot_instance_pools                      = 0
      on_demand_base_capacity                  = 0
      on_demand_percentage_above_base_capacity = lookup(each.value, "use_on_demand", true) ? 100 : 0
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
