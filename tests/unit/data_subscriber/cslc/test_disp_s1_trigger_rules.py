"""Pin the DISP-S1 trigger-rule wiring in conf/sds/rules/user_rules.json.

These are config-semantics tests: a silent revert of the rule JSON would
otherwise only surface as wasted or mis-routed evaluator jobs on a cluster.
"""

import fnmatch
import json
import os
import unittest

_RULES_PATH = os.path.normpath(os.path.join(
    os.path.dirname(__file__),
    "..", "..", "..", "..", "conf", "sds", "rules", "user_rules.json",
))


def _rules_by_name():
    with open(_RULES_PATH) as f:
        doc = json.load(f)
    items = []
    for v in doc.values():
        if isinstance(v, list):
            items.extend(r for r in v if isinstance(r, dict))
    return {r["rule_name"]: r for r in items if "rule_name" in r}


def _field(doc, path):
    value = doc
    for part in path.removesuffix(".keyword").split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    return value


def _same(a, b):
    return str(a).lower() == str(b).lower()


def _matches(query, doc):
    """Evaluate the query DSL subset the user rules use against one document."""
    if "bool" in query:
        b = query["bool"]
        return (all(_matches(q, doc) for q in b.get("must", []) + b.get("filter", []))
                and not any(_matches(q, doc) for q in b.get("must_not", []))
                and (not b.get("should") or any(_matches(q, doc) for q in b["should"])))
    if "term" in query:
        (path, value), = query["term"].items()
        return _same(_field(doc, path), value)
    if "terms" in query:
        (path, values), = query["terms"].items()
        return any(_same(_field(doc, path), v) for v in values)
    if "exists" in query:
        return _field(doc, query["exists"]["field"]) is not None
    raise AssertionError(f"query clause not modelled: {query}")


def _fired(rules, index, doc):
    """Names of the enabled grq rules a dataset published into index would trigger."""
    return [
        name for name, rule in rules.items()
        if rule.get("enabled") and "index_pattern" in rule
        and any(fnmatch.fnmatchcase(index, p.strip()) for p in rule["index_pattern"].split(","))
        and _matches(json.loads(rule["query_string"]), doc)
    ]


class TestDispS1TriggerRules(unittest.TestCase):

    def setUp(self):
        self.rules = _rules_by_name()

    def test_every_query_string_parses(self):
        for name, rule in self.rules.items():
            if "query_string" in rule:
                json.loads(rule["query_string"])  # raises on breakage

    def test_ksc_trigger_excludes_blackout_cscs(self):
        q = json.loads(
            self.rules["trigger-disp_s1_k_cycle_evaluator"]["query_string"]
        )
        must = q["bool"]["must"]
        self.assertIn({"term": {"metadata.is_complete": True}}, must)
        self.assertIn(
            {"term": {"metadata.blackout": True}},
            q["bool"].get("must_not", []),
            "blackout CSCs must not kick off k-cycle evaluations",
        )

    def test_cycle_evaluator_routed_to_private_queue(self):
        self.assertEqual(
            self.rules["trigger-disp_s1_cycle_evaluator"]["queue"],
            "opera-job_worker-evaluator_verdi",
        )

    def test_k_cycle_evaluator_stays_on_public_queue(self):
        # The k-cycle evaluator needs CMR (static layers) + CDDIS
        # (ionosphere) egress and must stay on the public evaluator queue.
        for name in ("trigger-disp_s1_k_cycle_evaluator",
                     "trigger-disp_s1_k_cycle_evaluator_on_ccslc"):
            self.assertEqual(
                self.rules[name]["queue"], "opera-job_worker-evaluator"
            )

    def test_sciflo_trigger_does_not_gate_on_large_gap(self):
        # large_gap is informational only — flag, never block.
        q = json.loads(self.rules["trigger-SCIFLO_L3_DISP_S1"]["query_string"])
        self.assertNotIn("large_gap", json.dumps(q))

    def test_ccslc_set_triggers_one_k_cycle_evaluation(self):
        # A k-boundary SCIFLO on a 27-burst frame publishes 27 CCSLCs and one set
        # marker. Triggering on each CCSLC started 27 concurrent evaluations of the
        # same KSCs; only the marker may start one.
        ccslcs = [
            ("grq_1_l2_cslc_s1_compressed-2026.10",
             {"dataset_type": "L2_CSLC_S1_COMPRESSED", "dataset": "L2_CSLC_S1_COMPRESSED",
              "metadata": {"frame_id": 36541, "burst_id": f"T064-{135500 + i}-IW1"}})
            for i in range(27)
        ]
        marker = ("grq_1_disp_s1-ccslc-set-2026.10",
                  {"dataset_type": "disp_s1-ccslc-set", "dataset": "disp_s1-ccslc-set",
                   "metadata": {"frame_id": 36541, "ccslc_count": 27}})

        evaluations = [
            name
            for index, doc in ccslcs + [marker]
            for name in _fired(self.rules, index, doc)
            if self.rules[name]["job_type"].startswith("hysds-io-disp_s1_k_cycle_evaluator:")
        ]
        self.assertEqual(evaluations, ["trigger-disp_s1_k_cycle_evaluator_on_ccslc"])

    def test_every_grq_rule_sets_enable_dedup(self):
        # Mozart stores enable_dedup: null when Figaro's rule editor saves a rule that
        # never had the key, and a null turns HySDS job dedup off for that rule. An
        # explicit true survives the edit.
        with open(_RULES_PATH) as f:
            grq_rules = json.load(f)["grq"]
        missing = [r["rule_name"] for r in grq_rules if r.get("enable_dedup") is not True]
        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
