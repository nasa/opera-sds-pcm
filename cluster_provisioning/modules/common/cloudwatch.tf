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
          "x":0,
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
                   "${var.amis["mozart"]}",
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
          "x":20,
          "y":40,
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
                   "${var.amis["mozart"]}",
                   "InstanceType",
                   "${var.mozart["instance_type"]}",
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
             "title":"CWAgent mozart disk usage"
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
                   "cpu_usage_iowait",
                   "InstanceId",
                   "${aws_instance.mozart.id}",
                   "ImageId",
                   "${var.amis["mozart"]}",
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
                   "${var.amis["mozart"]}",
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
                   "${var.amis["mozart"]}",
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
                   "${var.amis["mozart"]}",
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
  dimensions = {
    InstanceId = aws_instance.mozart.id
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
  dimensions = {
    InstanceId = aws_instance.factotum.id
  }
}

resource "aws_cloudwatch_metric_alarm" "mozart_memoryalarm" {
  alarm_name                = "CWAgent Memory"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "mem_used_percent"
  namespace                 = "CWAgent"
  period                    = "120"
  statistic                 = "Average"
  threshold                 = "90"
  alarm_description         = "This metric monitors mozart memory utilization"
  insufficient_data_actions = []
  dimensions = {
    InstanceId   = aws_instance.mozart.id
    ImageId      = var.amis["mozart"]
    InstanceType = var.mozart["instance_type"]
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
  dimensions = {
    InstanceId   = aws_instance.mozart.id
    ImageId      = var.amis["mozart"]
    InstanceType = var.mozart["instance_type"]
    device       = "nvme0n1p1"
    fstype       = "xfs"
    path         = "/"
  }
}

resource "aws_cloudwatch_metric_alarm" "sqs_cnm_r_dead_letter_alarm" {
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
    QueueName = aws_sqs_queue.cnm_response_dead_letter_queue.name
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
