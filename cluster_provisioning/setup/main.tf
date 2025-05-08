provider "aws" {
  shared_credentials_file = var.shared_credentials_file
  region                  = var.region
  profile                 = var.profile
}

resource "aws_sns_topic" "daac-proxy-for-opera" {
  name = "daac-proxy-for-opera"
}

resource "aws_sns_topic_policy" "daac-proxy-for-opera" {
  arn = aws_sns_topic.daac-proxy-for-opera.arn
  policy = data.aws_iam_policy_document.daac-proxy-for-opera.json
}

data "aws_iam_policy_document" "daac-proxy-for-opera" {
  policy_id = "__default_policy_ID"
  statement {
    actions = [
      "SNS:Publish",
      "SNS:RemovePermission",
      "SNS:SetTopicAttributes",
      "SNS:DeleteTopic",
      "SNS:ListSubscriptionsByTopic",
      "SNS:GetTopicAttributes",
      "SNS:Receive",
      "SNS:AddPermission",
      "SNS:Subscribe"
    ]
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = ["*"]
    }
    condition {
      test     = "StringEquals"
      variable = "AWS:SourceOwner"
      values = [
        var.aws_account
      ]
    }
    resources = [
      aws_sns_topic.daac-proxy-for-opera.arn
    ]
    sid = "__default_statement_ID"
  }
  statement {
    actions = [
      "SNS:Publish"
    ]
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${var.aws_account}:role/${var.pcm_verdi_role.name}"]
    }
    resources = [
      aws_sns_topic.daac-proxy-for-opera.arn
    ]
    sid = "__podaac_proxy_publish_statement_ID"
  }
}
