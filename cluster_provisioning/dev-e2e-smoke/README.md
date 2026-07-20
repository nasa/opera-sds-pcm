# Smoke Test E2E

End-to-end smoke test that provisions a PCM cluster, triggers processing for a
known set of inputs, and verifies that expected output products appear in GRQ.

Currently exercises **DSWx-HLS**. Designed to add more PGEs incrementally.

## How It Works

1. Jenkins provisions a fresh PCM cluster via Terraform (`main.tf` → `modules/common`)
2. `run_smoke_test.sh` runs on Mozart:
   - Disables PGE simulation mode
   - Invokes the L30 and S30 subscriber Lambdas with a known timestamp
   - Polls GRQ Elasticsearch until expected products appear (or timeout)
   - Verifies CNM-S was sent, mocks CNM-R, verifies delivery status
3. `check_pcm.py` asserts SUCCESS in the result files → JUnit XML
4. On success the cluster is auto-destroyed; on failure it's left alive for debugging

## Setup

Copy `smoke_test_e2e_infra.env.template` to `/data/home/hysdsops/smoke_test_e2e_infra.env`
on the Jenkins CI machine and fill in all values. This file is **not** committed to
version control.

## Adding a New PGE

### 1. `datasets_e2e.json`

Add a new segment with the expected output datasets:

```json
{
    "datasets": {
        "dswx_hls": [...],
        "cslc_s1": [
            {"dataset": "L2_CSLC_S1", "system_version": "0.1", "count": 5}
        ]
    }
}
```

| Field | Description |
|-------|-------------|
| `dataset` | The dataset type name as stored in GRQ (e.g. `L3_DSWx_HLS`, `L2_CSLC_S1`) |
| `system_version` | The version string in the GRQ index name. For example, `"2.0"` corresponds to the index `grq_v2.0_l3_dswx_hls-*`. Check the deployed GRQ indices to find the correct value for your product. |
| `count` | Number of products expected from the test inputs |

### 2. `run_smoke_test.sh`

Add a new phase section that triggers processing for your PGE (Lambda invocation,
job submission, etc.) and calls `check_datasets_file.py` with your new segment name:

```bash
# ============================================================
# Phase N: CSLC-S1 Processing
# ============================================================
~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py \
  --crid=${crid} \
  ${TEST_DIR}/datasets_e2e.json \
  cslc_s1 \
  --max_time 3600 \
  /tmp/datasets_cslc_s1.txt
```

### 3. `check_pcm.py`

Add a test method for the new result file:

```python
def test_cslc_s1_expected_datasets(self):
    logger = logging.getLogger(__name__)
    self.check_expected("/tmp/datasets_cslc_s1.txt", logger)
```

### 4. `verify_cnm.py`

Append product ID prefixes to the `--products` argument in `run_smoke_test.sh`.

### 5. `Jenkinsfile` / `main.tf`

No changes needed — they are PGE-agnostic.

## Items to Verify Before First Run

- **Lambda function names** — The script assumes the convention
  `{project}-{venue}-{counter}-opera-pcm-{l30|s30}-data-subscriber-query-timer`.
  Verify against the actual Terraform resource names in `modules/common/`.
- **`CNM_R_TOPIC_ARN`** — Must be populated in the infra env file. May also need
  to be added to `smoke_test_inputs.config` via the `local_file` resource in
  `modules/common/mozart.tf`.
- **`system_version`** — Confirm the GRQ index version matches what's in
  `datasets_e2e.json` (currently `"2.0"` for DSWx-HLS).
- **Worker ASG name** — Verify the autoscaling group naming pattern matches what
  Terraform creates (expected: `{project}-{venue}-{counter}-opera-job_worker-sciflo-l3_dswx_hls`).
- **Test granule availability** — The test invokes Lambdas with
  `time=2022-01-01T01:00:00Z`. This must return valid granules from CMR for
  tiles T54PVQ (L30/L8) and T53HQV (S30/S2A).
