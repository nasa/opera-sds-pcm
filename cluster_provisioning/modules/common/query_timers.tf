# Resources to provision the Data Subscriber timers
resource "aws_lambda_function" "hlsl30_query_timer" {
  depends_on = [null_resource.download_lambdas, aws_instance.mozart]
  filename = "${var.lambda_data-subscriber-query_handler_package_name}-${var.lambda_package_release}.zip"
  description = "Lambda function to submit a job that will query HLSL30 data."
  function_name = "${var.project}-${var.venue}-${local.counter}-hlsl30-query-timer"
  handler = "lambda_function.lambda_handler"
  role = var.lambda_role_arn
  runtime = "python3.8"
  vpc_config {
    security_group_ids = [var.cluster_security_group_id]
    subnet_ids = data.aws_subnet_ids.lambda_vpc.ids
  }
  timeout = 30
  environment {
    variables = {
      "MOZART_URL": "https://${aws_instance.mozart.private_ip}/mozart",
      "JOB_QUEUE": "opera-job_worker-hls_data_query",
      "JOB_TYPE": local.hlsl30_query_job_type,
      "JOB_RELEASE": var.pcm_branch,
      "MINUTES": var.hlsl30_query_timer_trigger_frequency,
      "PROVIDER": var.hls_provider,
	  "ENDPOINT": "OPS",
      "DOWNLOAD_JOB_QUEUE": "${var.project}-job_worker-hls_data_download",
      "CHUNK_SIZE": "1",
      "MAX_REVISION": "1000",
      "SMOKE_RUN": "false",
      "DRY_RUN": "false",
      "NO_SCHEDULE_DOWNLOAD": "false",
      "USE_TEMPORAL": "false",
      # set either or, but not both TEMPORAL_START_DATETIME and TEMPORAL_START_DATETIME_MARGIN_DAYS
      "TEMPORAL_START_DATETIME": "",
      "TEMPORAL_START_DATETIME_MARGIN_DAYS": "30",
      "REVISION_START_DATETIME_MARGIN_MINS": "0"
    }
  }
}
resource "aws_cloudwatch_log_group" "hlsl30_query_timer" {
  depends_on = [aws_lambda_function.hlsl30_query_timer]
  name = "/aws/lambda/${aws_lambda_function.hlsl30_query_timer.function_name}"
  retention_in_days = var.lambda_log_retention_in_days
}
resource "aws_lambda_function" "hlss30_query_timer" {
  depends_on = [null_resource.download_lambdas, aws_instance.mozart]
  filename = "${var.lambda_data-subscriber-query_handler_package_name}-${var.lambda_package_release}.zip"
  description = "Lambda function to submit a job that will query HLSS30 data"
  function_name = "${var.project}-${var.venue}-${local.counter}-hlss30-query-timer"
  handler = "lambda_function.lambda_handler"
  role = var.lambda_role_arn
  runtime = "python3.8"
  vpc_config {
    security_group_ids = [var.cluster_security_group_id]
    subnet_ids = data.aws_subnet_ids.lambda_vpc.ids
  }
  timeout = 30
  environment {
    variables = {
      "MOZART_URL": "https://${aws_instance.mozart.private_ip}/mozart",
      "JOB_QUEUE": "opera-job_worker-hls_data_query",
      "JOB_TYPE": local.hlss30_query_job_type,
      "JOB_RELEASE": var.pcm_branch,
      "PROVIDER": var.hls_provider,
	  "ENDPOINT": "OPS",
      "MINUTES": var.hlss30_query_timer_trigger_frequency,
      "DOWNLOAD_JOB_QUEUE": "${var.project}-job_worker-hls_data_download",
      "CHUNK_SIZE": "1",
      "MAX_REVISION": "1000",
      "SMOKE_RUN": "false",
      "DRY_RUN": "false",
      "NO_SCHEDULE_DOWNLOAD": "false",
      "USE_TEMPORAL": "false",
      # set either or, but not both TEMPORAL_START_DATETIME and TEMPORAL_START_DATETIME_MARGIN_DAYS
      "TEMPORAL_START_DATETIME": "",
      "TEMPORAL_START_DATETIME_MARGIN_DAYS": "30",
      "REVISION_START_DATETIME_MARGIN_MINS": "0"
    }
  }
}

resource "aws_cloudwatch_event_rule" "hlsl30_query_timer" {
  name = "${aws_lambda_function.hlsl30_query_timer.function_name}-Trigger"
  description = "Cloudwatch event to trigger the Data Subscriber Timer Lambda"
  schedule_expression = var.hlsl30_query_timer_trigger_frequency
  is_enabled = local.enable_download_timer
  depends_on = [null_resource.setup_trigger_rules]
}

resource "aws_cloudwatch_event_target" "hlsl30_query_timer" {
  rule = aws_cloudwatch_event_rule.hlsl30_query_timer.name
  target_id = "Lambda"
  arn = aws_lambda_function.hlsl30_query_timer.arn
}

resource "aws_lambda_permission" "hlsl30_query_timer" {
  statement_id = aws_cloudwatch_event_rule.hlsl30_query_timer.name
  action = "lambda:InvokeFunction"
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.hlsl30_query_timer.arn
  function_name = aws_lambda_function.hlsl30_query_timer.function_name
}

resource "aws_cloudwatch_log_group" "hlss30_query_timer" {
  depends_on = [aws_lambda_function.hlss30_query_timer]
  name = "/aws/lambda/${aws_lambda_function.hlss30_query_timer.function_name}"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_event_rule" "hlss30_query_timer" {
  name = "${aws_lambda_function.hlss30_query_timer.function_name}-Trigger"
  description = "Cloudwatch event to trigger the Data Subscriber Timer Lambda"
  schedule_expression = var.hlss30_query_timer_trigger_frequency
  is_enabled = local.enable_download_timer
  depends_on = [null_resource.setup_trigger_rules]
}

resource "aws_cloudwatch_event_target" "hlss30_query_timer" {
  rule = aws_cloudwatch_event_rule.hlss30_query_timer.name
  target_id = "Lambda"
  arn = aws_lambda_function.hlss30_query_timer.arn
}

resource "aws_lambda_permission" "hlss30_query_timer" {
  statement_id = aws_cloudwatch_event_rule.hlss30_query_timer.name
  action = "lambda:InvokeFunction"
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.hlss30_query_timer.arn
  function_name = aws_lambda_function.hlss30_query_timer.function_name
}

resource "aws_lambda_function" "slcs1a_query_timer" {
  depends_on = [null_resource.download_lambdas, aws_instance.mozart]
  filename = "${var.lambda_data-subscriber-query_handler_package_name}-${var.lambda_package_release}.zip"
  description = "Lambda function to submit a job that will query Sentinel SLC 1A data."
  function_name = "${var.project}-${var.venue}-${local.counter}-slcs1a-query-timer"
  handler = "lambda_function.lambda_handler"
  role = var.lambda_role_arn
  runtime = "python3.8"
  vpc_config {
    security_group_ids = [var.cluster_security_group_id]
    subnet_ids = data.aws_subnet_ids.lambda_vpc.ids
  }
  timeout = 30
  environment {
    variables = {
      "MOZART_URL": "https://${aws_instance.mozart.private_ip}/mozart",
      "JOB_QUEUE": "opera-job_worker-slc_data_query",
      "JOB_TYPE": local.slcs1a_query_job_type,
      "JOB_RELEASE": var.pcm_branch,
      "MINUTES": var.slcs1a_query_timer_trigger_frequency,
      "PROVIDER": var.slc_provider,
      "ENDPOINT": "OPS",
      "DOWNLOAD_JOB_QUEUE": "${var.project}-job_worker-slc_data_download",
      "CHUNK_SIZE": "1",
      "MAX_REVISION": "1000",
      "SMOKE_RUN": "false",
      "DRY_RUN": "false",
      "NO_SCHEDULE_DOWNLOAD": "false",
      "BOUNDING_BOX": ""
      "USE_TEMPORAL": "false",
      # set either or, but not both TEMPORAL_START_DATETIME and TEMPORAL_START_DATETIME_MARGIN_DAYS
      "TEMPORAL_START_DATETIME": "",
      "TEMPORAL_START_DATETIME_MARGIN_DAYS": "30",
      "REVISION_START_DATETIME_MARGIN_MINS": "0"
    }
  }
}
resource "aws_cloudwatch_log_group" "slcs1a_query_timer" {
  depends_on = [aws_lambda_function.slcs1a_query_timer]
  name = "/aws/lambda/${aws_lambda_function.slcs1a_query_timer.function_name}"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_event_rule" "slcs1a_query_timer" {
  name = "${aws_lambda_function.slcs1a_query_timer.function_name}-Trigger"
  description = "Cloudwatch event to trigger the Data Subscriber Timer Lambda"
  schedule_expression = var.slcs1a_query_timer_trigger_frequency
  is_enabled = local.enable_download_timer
  depends_on = [null_resource.setup_trigger_rules]
}

resource "aws_cloudwatch_event_target" "slcs1a_query_timer" {
  rule = aws_cloudwatch_event_rule.slcs1a_query_timer.name
  target_id = "Lambda"
  arn = aws_lambda_function.slcs1a_query_timer.arn
}

resource "aws_lambda_permission" "slcs1a_query_timer" {
  statement_id = aws_cloudwatch_event_rule.slcs1a_query_timer.name
  action = "lambda:InvokeFunction"
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.slcs1a_query_timer.arn
  function_name = aws_lambda_function.slcs1a_query_timer.function_name
}

resource "aws_lambda_function" "slc_ionosphere_download_timer" {
  depends_on = [null_resource.download_lambdas]
  filename = "${var.lambda_data-subscriber-download-slc-ionosphere_handler_package_name}-${var.lambda_package_release}.zip"
  description = "Lambda function to submit a job that will download ionosphere correction files for SLC products."
  function_name = "${var.project}-${var.venue}-${local.counter}-slc-ionosphere-download-timer"
  handler = "lambda_function.lambda_handler"
  role = var.lambda_role_arn
  runtime = "python3.8"
  vpc_config {
    security_group_ids = [var.cluster_security_group_id]
    subnet_ids = data.aws_subnet_ids.lambda_vpc.ids
  }
  timeout = 30
  environment {
    variables = {
      "MOZART_URL": "https://${aws_instance.mozart.private_ip}/mozart",
      "JOB_QUEUE": "opera-job_worker-slc_data_download_ionosphere",
      "JOB_TYPE": local.slc_ionosphere_download_job_type,
      "JOB_RELEASE": var.pcm_branch
      "QUERY_START_DATETIME_OFFSET_HOURS": "40"  # Plus QUERY_END offset
      "QUERY_END_DATETIME_OFFSET_HOURS": "12"    # 36h is the expected maximum latency for ionosphere availability for a *current* SLC product
    }
  }
}
resource "aws_cloudwatch_log_group" "slc_ionosphere_download_timer" {
  depends_on = [aws_lambda_function.slc_ionosphere_download_timer]
  name = "/aws/lambda/${aws_lambda_function.slc_ionosphere_download_timer.function_name}"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_event_rule" "slc_ionosphere_download_timer" {
  name = "${aws_lambda_function.slc_ionosphere_download_timer.function_name}-Trigger"
  description = "Cloudwatch event to trigger the Data Subscriber Ionosphere Download Timer Lambda"
  schedule_expression = var.slc_ionosphere_download_timer_trigger_frequency
  is_enabled = local.enable_download_timer
  depends_on = [null_resource.install_pcm_and_pges]
}

resource "aws_cloudwatch_event_target" "slc_ionosphere_download_timer" {
  rule = aws_cloudwatch_event_rule.slc_ionosphere_download_timer.name
  target_id = "Lambda"
  arn = aws_lambda_function.slc_ionosphere_download_timer.arn
}

resource "aws_lambda_permission" "slc_ionosphere_download_timer" {
  statement_id = aws_cloudwatch_event_rule.slc_ionosphere_download_timer.name
  action = "lambda:InvokeFunction"
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.slc_ionosphere_download_timer.arn
  function_name = aws_lambda_function.slc_ionosphere_download_timer.function_name
}

# Batch Query Lambda and Timer ---->

resource "aws_lambda_function" "batch_query_timer" {
  depends_on = [null_resource.download_lambdas, aws_instance.mozart]
  filename = "${var.lambda_batch-query_handler_package_name}-${var.lambda_package_release}.zip"
  description = "Lambda function to submit a job that will query batch data."
  function_name = "${var.project}-${var.venue}-${local.counter}-batch-query-timer"
  handler = "lambda_function.lambda_handler"
  role = var.lambda_role_arn
  runtime = "python3.8"
  vpc_config {
    security_group_ids = [var.cluster_security_group_id]
    subnet_ids = data.aws_subnet_ids.lambda_vpc.ids
  }
  timeout = 30
  environment {
    variables = {
      "MOZART_IP": "${aws_instance.mozart.private_ip}",
      "GRQ_IP": "${aws_instance.grq.private_ip}",
      "GRQ_ES_PORT": "9200",
      "ENDPOINT": "OPS",
      "JOB_RELEASE": var.pcm_branch
    }
  }
}
resource "aws_cloudwatch_log_group" "batch_query_timer" {
  depends_on = [aws_lambda_function.batch_query_timer]
  name = "/aws/lambda/${aws_lambda_function.batch_query_timer.function_name}"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_cloudwatch_event_rule" "batch_query_timer" {
  name = "${aws_lambda_function.batch_query_timer.function_name}-Trigger"
  description = "Cloudwatch event to trigger the Batch Timer Lambda"
  schedule_expression = var.batch_query_timer_trigger_frequency
  is_enabled = local.enable_download_timer
  depends_on = [null_resource.setup_trigger_rules]
}

resource "aws_cloudwatch_event_target" "batch_query_timer" {
  rule = aws_cloudwatch_event_rule.batch_query_timer.name
  target_id = "Lambda"
  arn = aws_lambda_function.batch_query_timer.arn
}

resource "aws_lambda_permission" "batch_query_timer" {
  statement_id = aws_cloudwatch_event_rule.batch_query_timer.name
  action = "lambda:InvokeFunction"
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.batch_query_timer.arn
  function_name = aws_lambda_function.batch_query_timer.function_name
}

# <------ Batch Query Lambda and Timer
