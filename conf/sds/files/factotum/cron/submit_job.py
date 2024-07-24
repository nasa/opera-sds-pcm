#!/usr/bin/env python

import uuid
import sys
import hashlib
import re
import yaml
from hysds_commons.job_utils import submit_mozart_job

# have yaml parse regular expressions
yaml.SafeLoader.add_constructor(
    u"tag:yaml.org,2002:python/regexp", lambda l, n: re.compile(l.construct_scalar(n))
)

settings = yaml.safe_load(open("/export/home/hysdsops/verdi/etc/settings.yaml"))
release_version = settings["RELEASE_VERSION"]
generated_uuid = str(uuid.uuid4())
payload_hash = hashlib.md5(generated_uuid.encode()).hexdigest()

result = submit_mozart_job(
        hysdsio={
            "id": generated_uuid,
            "params": [],
            "job-specification": f"job-submit_pending_jobs:{release_version}",
        },
        product={},
        rule={
            "rule_name": f"trigger-submit_pending_jobs",
            "queue": "factotum-job_worker-small", #"opera-job_worker-submit_pending_jobs"
            "priority": "0",
            "kwargs": "{}",
            "enable_dedup": True
        },
        queue=None,
        job_name="job-submit_pending_jobs",
        payload_hash=payload_hash, # we have to provide a unique payload hash so that this doesn't dedup
        enable_dedup=None,
        soft_time_limit=None,
        time_limit=None,
        component=None
    )

print(result)