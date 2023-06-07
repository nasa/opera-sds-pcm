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
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f",
                    "retention_in_days" : 30
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_hlss30_query.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_hlss30_query.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f",
                    "retention_in_days" : 30
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_hls_download.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_hls_download.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f",
                    "retention_in_days" : 30
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_slcs1a_query.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_slcs1a_query.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f",
                    "retention_in_days" : 30
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_slcs1b_query.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_slcs1b_query.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f",
                    "retention_in_days" : 30
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_slc_download.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_slc_download.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f",
                    "retention_in_days" : 30
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_batch_query.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_batch_query.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f",
                    "retention_in_days" : 30
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_pcm_int.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_pcm_int.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f",
                    "retention_in_days" : 30
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_on_demand.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_on_demand.log",
                    "timezone": "Local",
                    "retention_in_days" : 30
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_sciflo_L3_DSWx_HLS.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_sciflo_L3_DSWx_HLS.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S",
                    "retention_in_days" : 30
                  },
                  {
                    "file_path": "/data/work/jobs/**/run_sciflo_L2_CSLC_S1.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/run_sciflo_L2_CSLC_S1.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S",
                    "retention_in_days" : 30
                  },
                  {
                    "file_path": "/home/ops/verdi/log/opera-job_worker-hls_data_query.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/opera-job_worker-hls_data_query.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f",
                    "retention_in_days" : 30
                  },
                  {
                    "file_path": "/home/ops/verdi/log/opera-job_worker-hls_data_download.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/opera-job_worker-hls_data_download.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f",
                    "retention_in_days" : 30
                  },
                  {
                    "file_path": "/home/ops/verdi/log/opera-job_worker-slc_data_query.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/opera-job_worker-slc_data_query.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f",
                    "retention_in_days" : 30
                  },
                  {
                    "file_path": "/home/ops/verdi/log/opera-job_worker-slc_data_download.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/opera-job_worker-slc_data_download.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f",
                    "retention_in_days" : 30
                  },
                  {
                    "file_path": "/home/ops/verdi/log/opera-job_worker-hls_data_ingest.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/opera-job_worker-hls_data_ingest.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f",
                    "retention_in_days" : 30
                  },
                  {
                    "file_path": "/home/ops/verdi/log/opera-job_worker-sciflo-l3_dswx_hls.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/opera-job_worker-sciflo-l3_dswx_hls.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f",
                    "retention_in_days" : 30
                  },
                  {
                    "file_path": "/home/ops/verdi/log/opera-job_worker-sciflo-l2_cslc_s1.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/opera-job_worker-sciflo-l2_cslc_s1.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f",
                    "retention_in_days" : 30
                  },
                  {
                    "file_path": "/home/ops/verdi/log/opera-job_worker-sciflo-l2_rtc_s1.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/opera-job_worker-sciflo-l2_rtc_s1.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f",
                    "retention_in_days" : 30
                  },
                  {
                    "file_path": "/home/ops/verdi/log/opera-job_worker-send_cnm_notify.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/opera-job_worker-send_cnm_notify.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f",
                    "retention_in_days" : 30
                  },
                  {
                    "file_path": "/home/ops/verdi/log/opera-job_worker-rcv_cnm_notify.log",
                    "log_group_name": "/opera/sds/${var.project}-${var.venue}-${local.counter}/opera-job_worker-rcv_cnm_notify.log",
                    "timezone": "Local",
                    "timestamp_format": "%Y-%m-%d %H:%M:%S,%f",
                    "retention_in_days" : 30
                  }
                ]
              }
            },
            "force_flush_interval" : 15
          }
        }' > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
        