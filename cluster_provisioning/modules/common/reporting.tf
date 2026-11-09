resource "aws_lambda_function" "daac_delivery_report" {
  count      = var.cnm_accountability_reporting != null ? 1 : 0
  depends_on = [null_resource.download_lambdas, aws_instance.mozart]

  filename      = "${var.lambda_cnm_accountability_handler_package_name}-${var.lambda_package_release}.zip"
  description   = "Lambda to create an accountability report for product deliveries"
  function_name = "${var.project}-${var.venue}-${local.counter}-delivery_acc_lambda"
  handler       = "lambda_function.lambda_handler"
  role          = var.lambda_role_arn
  runtime       = "python3.14"
  vpc_config {
    security_group_ids = [var.cluster_security_group_id]
    subnet_ids         = data.aws_subnets.lambda_vpc.ids
  }
  timeout     = 900
  memory_size = 4096
  environment {
    variables = {
      "GRQ_URL" : "https://${aws_instance.mozart.private_ip}/grq_es",
      "VENUE" : upper("${var.project}-${var.venue}-${var.counter}"),
      "REPORT_SENDER_EMAIL" : var.cnm_accountability_reporting.sender,
      "REPORT_RECIPIENT_EMAILS" : join(",", var.cnm_accountability_reporting.recipients),
      "REPORT_CC_EMAILS" : join(",", var.cnm_accountability_reporting.cc),
      "REPORT_BCC_EMAILS" : join(",", var.cnm_accountability_reporting.bcc),
      "WINDOW_END_DAYS_BACK" : tostring(var.cnm_accountability_reporting.days_back),
      "WINDOW_SIZE_IN_DAYS" : tostring(var.cnm_accountability_reporting.window_size)
    }
  }
}
resource "aws_cloudwatch_log_group" "daac_delivery_report" {
  count             = var.cnm_accountability_reporting != null ? 1 : 0
  name              = "/aws/lambda/${aws_lambda_function.daac_delivery_report[0].function_name}"
  retention_in_days = var.lambda_log_retention_in_days
}
resource "aws_cloudwatch_event_rule" "daac_delivery_report" {
  count               = var.cnm_accountability_reporting != null ? 1 : 0
  name                = "${aws_lambda_function.daac_delivery_report[0].function_name}-Trigger"
  description         = "Cloudwatch event to trigger the DAAC accountability report lambda"
  schedule_expression = var.cnm_accountability_reporting.schedule
  state               = var.cnm_accountability_reporting.enabled ? "ENABLED" : "DISABLED"
}
resource "aws_cloudwatch_event_target" "daac_delivery_report" {
  count     = var.cnm_accountability_reporting != null ? 1 : 0
  arn       = aws_lambda_function.daac_delivery_report[0].arn
  target_id = "Lambda"
  rule      = aws_cloudwatch_event_rule.daac_delivery_report[0].name
}
resource "aws_lambda_permission" "daac_delivery_report" {
  count         = var.cnm_accountability_reporting != null ? 1 : 0
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.daac_delivery_report[0].function_name
  principal     = "events.amazonaws.com"
  statement_id  = aws_cloudwatch_event_rule.daac_delivery_report[0].name
  source_arn    = aws_cloudwatch_event_rule.daac_delivery_report[0].arn
}

resource "aws_lambda_function" "ops_report" {
  depends_on = [null_resource.download_lambdas]

  filename      = "${var.lambda_cnm_accountability_handler_package_name}-${var.lambda_package_release}.zip"
  description   = "Lambda to create a report of SDS status for OPS"
  function_name = "${var.project}-${var.venue}-${local.counter}-ops_reporting_lambda"
  handler       = "big_report_handler.lambda_handler"
  role          = var.lambda_role_arn
  runtime       = "python3.14"
  vpc_config {
    security_group_ids = [var.cluster_security_group_id]
    subnet_ids         = data.aws_subnets.lambda_vpc.ids
  }
  timeout     = 900
  memory_size = 4096
  ephemeral_storage {
    size = 2048
  }
  environment {
    variables = {
      # "VENUE" : upper("${var.project}-${var.venue}-${var.counter}"),
      "REPORT_SENDER_EMAIL" : var.ops_reporting.sender,
      "REPORT_RECIPIENT_EMAILS" : join(",", var.ops_reporting.recipients),
      "REPORT_CC_EMAILS" : join(",", var.ops_reporting.cc),
      "REPORT_BCC_EMAILS" : join(",", var.ops_reporting.bcc),
    }
  }
}
resource "aws_cloudwatch_log_group" "ops_report" {
  name              = "/aws/lambda/${aws_lambda_function.ops_report.function_name}"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "null_resource" "ops_reporting_enable" {
  count = var.ops_reporting.enabled ? 1 : 0
  depends_on = [aws_lambda_function.ops_report, aws_instance.mozart, null_resource.setup_cron_mozart]

  connection {
    type = "ssh"
    host = aws_instance.mozart.private_ip
    user = "hysdsops"
    private_key = file(var.private_key_file)
  }

  provisioner "remote-exec" {
    inline = [<<-EOT
      while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 10; done
      set -ex
      source ~/.bash_profile

      CRON="${var.ops_reporting.schedule} source mozart/bin/activate && python $HOME/mozart/ops/opera-pcm/tools/submit_ops_data_gather_job.py"

      (crontab -l 2>/dev/null; echo "$CRON") | crontab -

      echo "Enabled OPS reporting cron job"
    EOT
    ]
  }
}

