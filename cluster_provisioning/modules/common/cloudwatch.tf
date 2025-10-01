##############################
## CloudWatch Dashboard
##############################

resource "aws_cloudwatch_dashboard" "terraform-dashboard" {
  dashboard_name = "${var.project}-${var.venue}-${local.counter}-dashboard"

  dashboard_body = <<EOF
 {
   "widgets": [
       {
          "type":"metric",
          "x":0,
          "y":0,
          "width":12,
          "height":6,
          "properties":{
             "metrics":[
                [
                   "AWS/EC2",
                   "CPUUtilization",
                   "InstanceId",
                   "${aws_instance.mozart.id}"
                ]
             ],
             "period":60,
             "stat":"Average",
             "region":"${var.region}",
             "title":"${var.project}-${var.venue}-${local.counter}-mozart CPU"
          }
       },
       {
          "type":"metric",
          "x":20,
          "y":0,
          "width":12,
          "height":6,
          "properties":{
             "metrics":[
                [
                   "AWS/EC2",
                   "CPUUtilization",
                   "InstanceId",
                   "${aws_instance.mozart_es.id}"
                ]
             ],
             "period":60,
             "stat":"Average",
             "region":"${var.region}",
             "title":"${var.project}-${var.venue}-${local.counter}-mozart_es CPU"
          }
       },
       {
          "type":"metric",
          "x":0,
          "y":20,
          "width":12,
          "height":6,
          "properties":{
             "metrics":[
                [
                   "AWS/EC2",
                   "CPUUtilization",
                   "InstanceId",
                   "${aws_instance.metrics.id}"
                ]
             ],
             "period":60,
             "stat":"Average",
             "region":"${var.region}",
             "title":"${var.project}-${var.venue}-${local.counter}-metrics CPU"
          }
          },
          {
          "type":"metric",
          "x":20,
          "y":20,
          "width":12,
          "height":6,
          "properties":{
             "metrics":[
                [
                   "AWS/EC2",
                   "CPUUtilization",
                   "InstanceId",
                   "${aws_instance.grq.id}"
                ]
             ],
             "period":60,
             "stat":"Average",
             "region":"${var.region}",
             "title":"${var.project}-${var.venue}-${local.counter}-grq CPU"
          }
       },
       {
          "type":"metric",
          "x":0,
          "y":40,
          "width":12,
          "height":6,
          "properties":{
             "metrics":[
                [
                   "AWS/EC2",
                   "CPUUtilization",
                   "InstanceId",
                   "${aws_instance.factotum.id}"
                ]
             ],
             "period":60,
             "stat":"Average",
             "region":"${var.region}",
             "title":"${var.project}-${var.venue}-${local.counter}-factotum CPU"
          }
       },
       {
          "type":"metric",
          "x":20,
          "y":40,
          "width":12,
          "height":6,
          "properties":{
             "metrics":[
                [
                   "CWAgent",
                   "mem_used_percent",
                   "InstanceId",
                   "${aws_instance.mozart.id}",
                   "ImageId",
                   "${data.aws_ami.mozart_ami.id}",
                   "InstanceType",
                   "${var.mozart["instance_type"]}"
                ]
             ],
             "period":300,
             "stat":"Average",
             "region":"${var.region}",
             "title":"CWAgent mozart mem_used_percent"
          }
       },
       {
          "type":"metric",
          "x":0,
          "y":60,
          "width":12,
          "height":6,
          "properties":{
             "metrics":[
                [
                   "CWAgent",
                   "mem_used_percent",
                   "InstanceId",
                   "${aws_instance.mozart_es.id}",
                   "ImageId",
                   "${data.aws_ami.mozart_ami.id}",
                   "InstanceType",
                   "${var.mozart_es["instance_type"]}"
                ]
             ],
             "period":300,
             "stat":"Average",
             "region":"${var.region}",
             "title":"CWAgent mozart_es mem_used_percent"
          }
       },
       {
          "type":"metric",
          "x":0,
          "y":80,
          "width":12,
          "height":6,
          "properties":{
             "metrics":[
                [
                   "CWAgent",
                   "mem_used_percent",
                   "InstanceId",
                   "${aws_instance.grq.id}",
                   "ImageId",
                   "${data.aws_ami.grq_ami.id}",
                   "InstanceType",
                   "${var.grq["instance_type"]}"
                ]
             ],
             "period":300,
             "stat":"Average",
             "region":"${var.region}",
             "title":"CWAgent grq mem_used_percent"
          }
       },
       {
          "type":"metric",
          "x":20,
          "y":80,
          "width":12,
          "height":6,
          "properties":{
             "metrics":[
                [
                   "CWAgent",
                   "mem_used_percent",
                   "InstanceId",
                   "${aws_instance.metrics.id}",
                   "ImageId",
                   "${data.aws_ami.metrics_ami.id}",
                   "InstanceType",
                   "${var.metrics["instance_type"]}"
                ]
             ],
             "period":300,
             "stat":"Average",
             "region":"${var.region}",
             "title":"CWAgent metrics mem_used_percent"
          }
       },
       {
          "type":"metric",
          "x":0,
          "y":100,
          "width":12,
          "height":6,
          "properties":{
             "metrics":[
                [
                   "CWAgent",
                   "mem_used_percent",
                   "InstanceId",
                   "${aws_instance.factotum.id}",
                   "ImageId",
                   "${data.aws_ami.factotum_ami.id}",
                   "InstanceType",
                   "${var.factotum["instance_type"]}"
                ]
             ],
             "period":300,
             "stat":"Average",
             "region":"${var.region}",
             "title":"CWAgent factotum mem_used_percent"
          }
       },
       {
          "type":"metric",
          "x":20,
          "y":100,
          "width":12,
          "height":6,
          "properties":{
             "metrics":[
                [
                   "CWAgent",
                   "disk_used_percent",
                   "InstanceId",
                   "${aws_instance.mozart.id}",
                   "ImageId",
                   "${data.aws_ami.mozart_ami.id}",
                   "InstanceType",
                   "${var.mozart["instance_type"]}",
                   "fstype",
                   "xfs",
                   "device",
                   "nvme0n1p1",
                   "path",
                   "/"
                ],
                [
                   "CWAgent",
                   "disk_used_percent",
                   "InstanceId",
                   "${aws_instance.mozart.id}",
                   "ImageId",
                   "${data.aws_ami.mozart_ami.id}",
                   "InstanceType",
                   "${var.mozart["instance_type"]}",
                   "fstype",
                   "xfs",
                   "device",
                   "nvme1n1",
                   "path",
                   "/scratch"
                ]
             ],
             "period":300,
             "stat":"Average",
             "region":"${var.region}",
             "title":"CWAgent mozart disk usage"
          }
       },
       {
          "type":"metric",
          "x":0,
          "y":120,
          "width":12,
          "height":6,
          "properties":{
             "metrics":[
                [
                   "CWAgent",
                   "disk_used_percent",
                   "InstanceId",
                   "${aws_instance.mozart_es.id}",
                   "ImageId",
                   "${data.aws_ami.mozart_ami.id}",
                   "InstanceType",
                   "${var.mozart_es["instance_type"]}",
                   "fstype",
                   "xfs",
                   "device",
                   "nvme0n1p1",
                   "path",
                   "/"
                ]
             ],
             "period":300,
             "stat":"Average",
             "region":"${var.region}",
             "title":"CWAgent mozart_es disk usage"
          }
       },
       {
          "type":"metric",
          "x":20,
          "y":120,
          "width":12,
          "height":6,
          "properties":{
             "metrics":[
                [
                   "CWAgent",
                   "disk_used_percent",
                   "InstanceId",
                   "${aws_instance.grq.id}",
                   "ImageId",
                   "${data.aws_ami.grq_ami.id}",
                   "InstanceType",
                   "${var.grq["instance_type"]}",
                   "fstype",
                   "xfs",
                   "device",
                   "nvme0n1p1",
                   "path",
                   "/"
                ]
             ],
             "period":300,
             "stat":"Average",
             "region":"${var.region}",
             "title":"CWAgent grq disk usage"
          }
       },
       {
          "type":"metric",
          "x":0,
          "y":140,
          "width":12,
          "height":6,
          "properties":{
             "metrics":[
                [
                   "CWAgent",
                   "disk_used_percent",
                   "InstanceId",
                   "${aws_instance.factotum.id}",
                   "ImageId",
                   "${data.aws_ami.factotum_ami.id}",
                   "InstanceType",
                   "${var.factotum["instance_type"]}",
                   "fstype",
                   "xfs",
                   "device",
                   "nvme0n1p1",
                   "path",
                   "/"
                ]
             ],
             "period":300,
             "stat":"Average",
             "region":"${var.region}",
             "title":"CWAgent factotum disk usage"
          }
       },
       {
          "type":"metric",
          "x":20,
          "y":140,
          "width":12,
          "height":6,
          "properties":{
             "metrics":[
                [
                   "CWAgent",
                   "disk_used_percent",
                   "InstanceId",
                   "${aws_instance.metrics.id}",
                   "ImageId",
                   "${data.aws_ami.metrics_ami.id}",
                   "InstanceType",
                   "${var.metrics["instance_type"]}",
                   "fstype",
                   "xfs",
                   "device",
                   "nvme0n1p1",
                   "path",
                   "/"
                ]
             ],
             "period":300,
             "stat":"Average",
             "region":"${var.region}",
             "title":"CWAgent metrics disk usage"
          }
       },
       {
          "type":"metric",
          "x":0,
          "y":160,
          "width":12,
          "height":6,
          "properties":{
             "metrics":[
                [
                   "CWAgent",
                   "cpu_usage_iowait",
                   "InstanceId",
                   "${aws_instance.mozart.id}",
                   "ImageId",
                   "${data.aws_ami.mozart_ami.id}",
                   "InstanceType",
                   "${var.mozart["instance_type"]}",
                   "cpu",
                   "cpu1"
                ],
                [
                   "CWAgent",
                   "cpu_usage_iowait",
                   "InstanceId",
                   "${aws_instance.mozart.id}",
                   "ImageId",
                   "${data.aws_ami.mozart_ami.id}",
                   "InstanceType",
                   "${var.mozart["instance_type"]}",
                   "cpu",
                   "cpu2"
                ],
                                [
                   "CWAgent",
                   "cpu_usage_iowait",
                   "InstanceId",
                   "${aws_instance.mozart.id}",
                   "ImageId",
                   "${data.aws_ami.mozart_ami.id}",
                   "InstanceType",
                   "${var.mozart["instance_type"]}",
                   "cpu",
                   "cpu3"
                ],
                                [
                   "CWAgent",
                   "cpu_usage_iowait",
                   "InstanceId",
                   "${aws_instance.mozart.id}",
                   "ImageId",
                   "${data.aws_ami.mozart_ami.id}",
                   "InstanceType",
                   "${var.mozart["instance_type"]}",
                   "cpu",
                   "cpu4"
                ]
             ],
             "period":300,
             "stat":"Average",
             "region":"${var.region}",
             "title":"CWAgent cpu_usage_iowait"
          }
       },
       {
          "type":"log",
          "x":20,
          "y":160,
          "width":12,
          "height":6,
          "properties":{
             "query": "SOURCE 'CloudTrail/ManagementAPI' | fields @message, requestParameters.CreateFleetRequest.TargetCapacitySpecification.SpotTargetCapacity as SpotInstancesRequested\n| filter eventName=\"CreateFleet\"\n| parse @message '\"${var.project}-${var.venue}-${local.counter}\"' as venue\n| filter ispresent(venue)\n| stats sum(SpotInstancesRequested) by bin(60s)",
             "region":"${var.region}",
             "title":"${var.project}-${var.venue}-${local.counter} Spot Instances Requested (binned by minute)",
             "view":"timeSeries"
          }
       },
       {
          "type": "log",
          "x":20,
          "y":180,
          "width": 12,
          "height": 6,
          "properties": {
             "query": "SOURCE 'CloudTrail/ManagementAPI' | fields eventName\n| filter eventName=\"BidEvictedEvent\"\n| stats count(*) by bin(60s)",
             "region":"${var.region}",
             "title":"${var.project}-${var.venue}-${local.counter} Spot Instance Bid Evictions (binned by minute)",
             "view": "timeSeries"
          }
       },
       {
          "type":"metric",
          "width":12,
          "height":6,
          "properties": {
             "metrics": [
                [
                   "HySDS",
                   "${var.project}-${var.venue}-${local.counter}-opensearch_shards_usage"
                ]
             ],
             "period":60,
             "stat":"Average",
             "region":"${var.region}",
             "title":"${var.project}-${var.venue}-${local.counter}-OpenSearch Shards Usage"
          }
      }
   ]
 }
 EOF
}


##############################
## Alarms
##############################

resource "aws_cloudwatch_metric_alarm" "mozart_cpualarm" {
  alarm_name                = "${var.project}-${var.venue}-${local.counter}-mozart CPU"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "2"
  metric_name               = "CPUUtilization"
  namespace                 = "AWS/EC2"
  period                    = "120"
  statistic                 = "Average"
  threshold                 = "90"
  alarm_description         = "This metric monitors mozart cpu utilization"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.operator_notify.arn]
  dimensions = {
    InstanceId = aws_instance.mozart.id
  }
}

resource "aws_cloudwatch_metric_alarm" "mozart_es_cpualarm" {
  alarm_name                = "${var.project}-${var.venue}-${local.counter}-mozart_es CPU"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "2"
  metric_name               = "CPUUtilization"
  namespace                 = "AWS/EC2"
  period                    = "120"
  statistic                 = "Average"
  threshold                 = "90"
  alarm_description         = "This metric monitors mozart cpu utilization"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.operator_notify.arn]
  dimensions = {
    InstanceId = aws_instance.mozart_es.id
  }
}

resource "aws_cloudwatch_metric_alarm" "metrics_cpualarm" {
  alarm_name                = "${var.project}-${var.venue}-${local.counter}-metrics CPU"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "2"
  metric_name               = "CPUUtilization"
  namespace                 = "AWS/EC2"
  period                    = "120"
  statistic                 = "Average"
  threshold                 = "90"
  alarm_description         = "This metric monitors metrics cpu utilization"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.operator_notify.arn]
  dimensions = {
    InstanceId = aws_instance.metrics.id
  }
}

resource "aws_cloudwatch_metric_alarm" "grq_cpualarm" {
  alarm_name                = "${var.project}-${var.venue}-${local.counter}-grq CPU"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "2"
  metric_name               = "CPUUtilization"
  namespace                 = "AWS/EC2"
  period                    = "120"
  statistic                 = "Average"
  threshold                 = "90"
  alarm_description         = "This metric monitors grq cpu utilization"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.operator_notify.arn]
  dimensions = {
    InstanceId = aws_instance.grq.id
  }
}

resource "aws_cloudwatch_metric_alarm" "factotum_cpualarm" {
  alarm_name                = "${var.project}-${var.venue}-${local.counter}-factotum CPU"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "2"
  metric_name               = "CPUUtilization"
  namespace                 = "AWS/EC2"
  period                    = "120"
  statistic                 = "Average"
  threshold                 = "90"
  alarm_description         = "This metric monitors factotum cpu utilization"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.operator_notify.arn]
  dimensions = {
    InstanceId = aws_instance.factotum.id
  }
}

resource "aws_cloudwatch_metric_alarm" "mozart_memoryalarm" {
  alarm_name                = "${var.project}-${var.venue}-${local.counter}-mozart Memory"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "mem_used_percent"
  namespace                 = "CWAgent"
  period                    = "120"
  statistic                 = "Average"
  threshold                 = "90"
  alarm_description         = "This metric monitors mozart memory utilization"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.operator_notify.arn]
  dimensions = {
    InstanceId   = aws_instance.mozart.id
    ImageId      = data.aws_ami.mozart_ami.id
    InstanceType = var.mozart["instance_type"]
  }
}

resource "aws_cloudwatch_metric_alarm" "mozart_es_memoryalarm" {
  alarm_name                = "${var.project}-${var.venue}-${local.counter}-mozart_es Memory"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "mem_used_percent"
  namespace                 = "CWAgent"
  period                    = "120"
  statistic                 = "Average"
  threshold                 = "90"
  alarm_description         = "This metric monitors mozart_es memory utilization"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.operator_notify.arn]
  dimensions = {
    InstanceId   = aws_instance.mozart_es.id
    ImageId      = data.aws_ami.mozart_ami.id
    InstanceType = var.mozart_es["instance_type"]
  }
}

resource "aws_cloudwatch_metric_alarm" "grq_memoryalarm" {
  alarm_name                = "${var.project}-${var.venue}-${local.counter}-grq Memory"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "mem_used_percent"
  namespace                 = "CWAgent"
  period                    = "120"
  statistic                 = "Average"
  threshold                 = "90"
  alarm_description         = "This metric monitors grq memory utilization"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.operator_notify.arn]
  dimensions = {
    InstanceId   = aws_instance.grq.id
    ImageId      = data.aws_ami.grq_ami.id
    InstanceType = var.grq["instance_type"]
  }
}

resource "aws_cloudwatch_metric_alarm" "metrics_memoryalarm" {
  alarm_name                = "${var.project}-${var.venue}-${local.counter}-metrics Memory"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "mem_used_percent"
  namespace                 = "CWAgent"
  period                    = "120"
  statistic                 = "Average"
  threshold                 = "90"
  alarm_description         = "This metric monitors metrics memory utilization"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.operator_notify.arn]
  dimensions = {
    InstanceId   = aws_instance.metrics.id
    ImageId      = data.aws_ami.metrics_ami.id
    InstanceType = var.metrics["instance_type"]
  }
}

resource "aws_cloudwatch_metric_alarm" "factotum_memoryalarm" {
  alarm_name                = "${var.project}-${var.venue}-${local.counter}-factotum Memory"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "mem_used_percent"
  namespace                 = "CWAgent"
  period                    = "120"
  statistic                 = "Average"
  threshold                 = "90"
  alarm_description         = "This metric monitors factotum memory utilization"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.operator_notify.arn]
  dimensions = {
    InstanceId   = aws_instance.factotum.id
    ImageId      = data.aws_ami.factotum_ami.id
    InstanceType = var.factotum["instance_type"]
  }
}

resource "aws_cloudwatch_metric_alarm" "mozart_diskalarm" {
  alarm_name                = "${var.project}-${var.venue}-${local.counter}-mozart disk usage"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "disk_used_percent"
  namespace                 = "CWAgent"
  period                    = "120"
  statistic                 = "Average"
  threshold                 = "75"
  alarm_description         = "This metric monitors mozart disk utilization"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.operator_notify.arn]
  dimensions = {
    InstanceId   = aws_instance.mozart.id
    ImageId      = data.aws_ami.mozart_ami.id
    InstanceType = var.mozart["instance_type"]
    device       = "nvme0n1p1"
    fstype       = "xfs"
    path         = "/"
  }
}

resource "aws_cloudwatch_metric_alarm" "mozart_scratch_diskalarm" {
  alarm_name                = "${var.project}-${var.venue}-${local.counter}-mozart scratch disk usage"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "disk_used_percent"
  namespace                 = "CWAgent"
  period                    = "120"
  statistic                 = "Average"
  threshold                 = "75"
  alarm_description         = "This metric monitors mozart scratch disk utilization"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.operator_notify.arn]
  dimensions = {
    InstanceId   = aws_instance.mozart.id
    ImageId      = data.aws_ami.mozart_ami.id
    InstanceType = var.mozart["instance_type"]
    device       = "nvme1n1"
    fstype       = "xfs"
    path         = "/scratch"
  }
}

resource "aws_cloudwatch_metric_alarm" "mozart_es_diskalarm" {
  alarm_name                = "${var.project}-${var.venue}-${local.counter}-mozart_es disk usage"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "disk_used_percent"
  namespace                 = "CWAgent"
  period                    = "120"
  statistic                 = "Average"
  threshold                 = "75"
  alarm_description         = "This metric monitors mozart_es disk utilization"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.operator_notify.arn]
  dimensions = {
    InstanceId   = aws_instance.mozart_es.id
    ImageId      = data.aws_ami.mozart_ami.id
    InstanceType = var.mozart_es["instance_type"]
    device       = "nvme0n1p1"
    fstype       = "xfs"
    path         = "/"
  }
}

resource "aws_cloudwatch_metric_alarm" "grq_diskalarm" {
  alarm_name                = "${var.project}-${var.venue}-${local.counter}-grq disk usage"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "disk_used_percent"
  namespace                 = "CWAgent"
  period                    = "120"
  statistic                 = "Average"
  threshold                 = "75"
  alarm_description         = "This metric monitors grq disk utilization"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.operator_notify.arn]
  dimensions = {
    InstanceId   = aws_instance.grq.id
    ImageId      = data.aws_ami.grq_ami.id
    InstanceType = var.grq["instance_type"]
    device       = "nvme0n1p1"
    fstype       = "xfs"
    path         = "/"
  }
}

resource "aws_cloudwatch_metric_alarm" "metrics_diskalarm" {
  alarm_name                = "${var.project}-${var.venue}-${local.counter}-metrics disk usage"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "disk_used_percent"
  namespace                 = "CWAgent"
  period                    = "120"
  statistic                 = "Average"
  threshold                 = "75"
  alarm_description         = "This metric monitors metrics disk utilization"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.operator_notify.arn]
  dimensions = {
    InstanceId   = aws_instance.metrics.id
    ImageId      = data.aws_ami.metrics_ami.id
    InstanceType = var.metrics["instance_type"]
    device       = "nvme0n1p1"
    fstype       = "xfs"
    path         = "/"
  }
}

resource "aws_cloudwatch_metric_alarm" "factotum_diskalarm" {
  alarm_name                = "${var.project}-${var.venue}-${local.counter}-factotum disk usage"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "disk_used_percent"
  namespace                 = "CWAgent"
  period                    = "120"
  statistic                 = "Average"
  threshold                 = "75"
  alarm_description         = "This metric monitors factotum disk utilization"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.operator_notify.arn]
  dimensions = {
    InstanceId   = aws_instance.factotum.id
    ImageId      = data.aws_ami.factotum_ami.id
    InstanceType = var.factotum["instance_type"]
    device       = "nvme0n1p1"
    fstype       = "xfs"
    path         = "/"
  }
}

resource "aws_cloudwatch_metric_alarm" "sqs_cnm_r_dead_letter_alarm" {
  count                     = local.sqs_count
  alarm_name                = "${var.project}-${var.venue}-${local.counter}-mozart CNM-R dead letter queue"
  depends_on                = [aws_sqs_queue.cnm_response_dead_letter_queue]
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "2"
  metric_name               = "ApproximateNumberOfMessagesVisible"
  namespace                 = "AWS/SQS"
  period                    = "300"
  statistic                 = "Average"
  threshold                 = "5"
  alarm_description         = "This metric monitors size of CNM-R dead letter queue"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.operator_notify.arn]
  dimensions = {
    QueueName = aws_sqs_queue.cnm_response_dead_letter_queue[count.index].name
  }
}

resource "aws_cloudwatch_metric_alarm" "sqs_dead_letter_alarm" {
  alarm_name                = "${var.project}-${var.venue}-${local.counter}-mozart ISL dead letter queue"
  depends_on                = [aws_sqs_queue.isl_dead_letter_queue]
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "2"
  metric_name               = "ApproximateNumberOfMessagesVisible"
  namespace                 = "AWS/SQS"
  period                    = "300"
  statistic                 = "Average"
  threshold                 = "5"
  alarm_description         = "This metric monitors size of isl dead letter queue"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.operator_notify.arn]
  dimensions = {
    QueueName = aws_sqs_queue.isl_dead_letter_queue.name
  }
}

resource "aws_cloudwatch_metric_alarm" "sqs_event_misfire_alarm" {
  alarm_name                = "${var.project}-${var.venue}-${local.counter}-event-misfire"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "NumberOfMissedFiles"
  namespace                 = "AWS/Lambda"
  period                    = "60"
  statistic                 = "Average"
  threshold                 = "1"
  alarm_description         = "This metric monitors size of input files in ${local.isl_bucket} missed for firing events"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.operator_notify.arn]
  dimensions = {
    LAMBDA_NAME                 = "event-misfire_lambda"
    E_MISFIRE_METRIC_ALARM_NAME = "${var.project}-${var.venue}-${local.counter}-event-misfire"
  }
}

resource "aws_cloudwatch_metric_alarm" "opensearch_shards_usage_alarm" {
  alarm_name                = "${var.project}-${var.venue}-${local.counter}-opensearch shards usage"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "${var.project}-${var.venue}-${local.counter}-opensearch_shards_usage"
  namespace                 = "HySDS"
  period                    = "120"
  statistic                 = "Average"
  threshold                 = "90"
  alarm_description         = "This alarm alerts the operator when the OpenSearch shards utilization exceeds a threshold"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.operator_notify.arn]
}

