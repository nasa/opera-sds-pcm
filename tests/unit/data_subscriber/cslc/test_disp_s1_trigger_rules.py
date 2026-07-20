"""Pin the DISP-S1 trigger-rule wiring in conf/sds/rules/user_rules.json.

These are config-semantics tests: a silent revert of the rule JSON would
otherwise only surface as wasted or mis-routed evaluator jobs on a cluster.
"""

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


if __name__ == "__main__":
    unittest.main()
