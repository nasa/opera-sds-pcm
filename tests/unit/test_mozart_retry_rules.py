"""Pin the Mozart retry rules in conf/sds/rules/user_rules-mozart.json.

A retry rule is a query over a failed job's status document. These tests run each rule's
query against job documents shaped like the failures the rule was written for, so a rule
that stops matching its failure, or starts matching others, shows up here and not as a
frame that quietly stopped on a cluster.
"""

import json
import os
import re

import pytest

RULES_PATH = os.path.normpath(os.path.join(
    os.path.dirname(__file__), "..", "..", "conf", "sds", "rules", "user_rules-mozart.json"))
LW_TAG = "__LW_TAG__"


def rules():
    with open(RULES_PATH) as f:
        return json.load(f)["mozart"]


def rule(name):
    found = [r for r in rules() if r["rule_name"] == name]
    assert len(found) == 1, f"expected one rule named {name}, found {len(found)}"
    return found[0]


def _text(doc, field):
    value = doc.get(field.removesuffix(".keyword"))
    return "" if value is None else str(value)


def _phrase_in(phrase, text):
    """A phrase match, near enough for these documents: the words in order, case ignored."""
    words = re.findall(r"[\w.]+", phrase.lower())
    return " ".join(words) in " ".join(re.findall(r"[\w.]+", text.lower()))


def matches(query, doc):
    """Evaluate the query DSL subset the retry rules use against one job status document."""
    if "bool" in query:
        b = query["bool"]
        return (all(matches(q, doc) for q in b.get("must", []))
                and (not b.get("should") or any(matches(q, doc) for q in b["should"])))
    if "term" in query:
        (field, value), = query["term"].items()
        return _text(doc, field) == value
    if "prefix" in query:
        (field, value), = query["prefix"].items()
        return _text(doc, field).startswith(value)
    if "match_phrase" in query:
        (field, phrase), = query["match_phrase"].items()
        return _phrase_in(phrase, _text(doc, field))
    if "query_string" in query:
        # the rules only use a quoted phrase here, searched across every field
        phrase = query["query_string"]["query"].strip('"')
        return any(_phrase_in(phrase, str(value)) for value in doc.values())
    raise AssertionError(f"query clause not modelled: {query}")


def fired(doc):
    """Names of the enabled rules that would retry this job."""
    return sorted(r["rule_name"] for r in rules()
                  if r["enabled"] and matches(json.loads(r["query_string"]), doc))


def failed_job(job_type, error, traceback=""):
    return {"status": "job-failed", "resource": "job", "type": job_type, "error": error,
            "short_error": f"{error[:20]}.....{error[-10:]}", "traceback": traceback}


# ---------------------------------------------------------------------------
# the file as a whole
# ---------------------------------------------------------------------------

def test_every_query_string_is_a_query():
    for r in rules():
        assert isinstance(json.loads(r["query_string"]), dict), r["rule_name"]


def test_rule_names_are_unique():
    names = [r["rule_name"] for r in rules()]
    assert len(names) == len(set(names))


def test_every_rule_retries_through_the_lightweight_job():
    for r in rules():
        for field in ("job_spec", "job_type", "workflow"):
            assert r[field].endswith(f"lw-mozart-retry:{LW_TAG}"), (r["rule_name"], field)
        assert r["queue"] == "system-jobs-queue", r["rule_name"]
        assert int(json.loads(r["kwargs"])["retry_count_max"]) > 0, r["rule_name"]


def test_the_release_tag_placeholder_appears_once_per_line():
    """The rule import substitutes the placeholder with a sed that replaces one per line."""
    with open(RULES_PATH) as f:
        for number, line in enumerate(f, 1):
            assert line.count(LW_TAG) <= 1, f"line {number} has {line.count(LW_TAG)} placeholders"


# ---------------------------------------------------------------------------
# two catalog ingest jobs publishing one CSLC at the same moment
#
# Frames 44045 and 44046 share burst T165-352361-IW1. Their ingest jobs for one date ran four
# seconds apart; the 44045 job found the granule locked by the other, went to read the lock,
# and the lock was gone. Its whole publish was aborted.
# ---------------------------------------------------------------------------

PUBLISH_LOCK_RULE = "retry-cslc_catalog_ingest-publish_lock"
PUBLISH_LOCK_TRACEBACK = """Traceback (most recent call last):
  File "/root/verdi/ops/osaka-1.3.2/osaka/cooperator.py", line 38, in __enter__
    self.dlock.lock(lockMetadata)
  File "/root/verdi/ops/osaka-1.3.2/osaka/lock.py", line 78, in lock
    raise osaka.utils.OsakaException("Lock file already locked")
osaka.utils.OsakaException: Lock file already locked

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/root/verdi/ops/hysds-3.3.5/hysds/dataset_ingest_bulk.py", line 179, in write_to_object_store
    osaka.main.put(publ_ctx_file, publ_ctx_url, params=params, noclobber=True)
  File "/root/verdi/ops/osaka-1.3.2/osaka/cooperator.py", line 66, in whenLocked
    ex_source = self.dlock.getLockMetadata("source")
botocore.errorfactory.NoSuchKey: An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
"""
PUBLISH_LOCK_ERROR = ("Product failed to ingest to data store: An error occurred (NoSuchKey) when "
                      "calling the GetObject operation: The specified key does not exist.")


def test_publish_lock_collision_of_a_catalog_ingest_job_is_retried():
    job = failed_job("job-cslc_catalog_ingest:6.0.8", PUBLISH_LOCK_ERROR, PUBLISH_LOCK_TRACEBACK)

    assert fired(job) == [PUBLISH_LOCK_RULE]


def test_publish_lock_rule_does_not_depend_on_the_release():
    job = failed_job("job-cslc_catalog_ingest:develop", PUBLISH_LOCK_ERROR, PUBLISH_LOCK_TRACEBACK)

    assert PUBLISH_LOCK_RULE in fired(job)


def test_catalog_ingest_job_that_failed_for_another_reason_is_not_retried_by_it():
    job = failed_job("job-cslc_catalog_ingest:6.0.8",
                     "CMR gap-check query failed", "RuntimeError: CMR 503 Service Unavailable")

    assert PUBLISH_LOCK_RULE not in fired(job)


@pytest.mark.parametrize("job_type", ["job-SCIFLO_L3_DISP_S1:6.0.8", "job-cslc_download:6.0.8",
                                      "job-SCIFLO_L2_RTC_S1:6.0.8"])
def test_publish_lock_collision_of_a_job_that_makes_products_is_not_retried(job_type):
    """Two jobs writing one product is a duplicate to look at, not a metadata-only race."""
    job = failed_job(job_type, PUBLISH_LOCK_ERROR, PUBLISH_LOCK_TRACEBACK)

    assert PUBLISH_LOCK_RULE not in fired(job)


@pytest.mark.parametrize("status", ["job-completed", "job-started", "job-queued"])
def test_publish_lock_rule_only_acts_on_a_failed_job(status):
    job = dict(failed_job("job-cslc_catalog_ingest:6.0.8", PUBLISH_LOCK_ERROR, PUBLISH_LOCK_TRACEBACK),
               status=status)

    assert fired(job) == []


def test_publish_lock_rule_allows_ten_retries():
    r = rule(PUBLISH_LOCK_RULE)

    assert json.loads(r["kwargs"]) == {"retry_count_max": "10", "job_priority_increment": "+1"}
    assert r["enabled"] is True
