import json
import os

try:
    import unittest.mock as umock
except ImportError:
    import mock as umock
import unittest

BASE_PATH = os.path.dirname(os.path.abspath(__file__))


class TestGetOrbitEphemeris(unittest.TestCase):

    # checks to verify that we're getting back the appropriate processing type value
    def setUp(self):
        ctx_file = os.path.join(BASE_PATH, "data", "_context.json")
        pge_config_file = os.path.join(BASE_PATH, "data", "pge_configs.json")
        job_params_file = os.path.join(BASE_PATH, "data", "job_params.json")
        recs_file = os.path.join(BASE_PATH, "data", "recs2check_json.json")
        best_fit_file = os.path.join(BASE_PATH, "data", "best_fit_record_json.json")

        self.settings = {}
        with open(ctx_file, "r") as ctx:
            self.context = json.load(ctx)
        with open(pge_config_file, "r") as pg:
            self.pge_config = json.load(pg)
        with open(job_params_file, "r") as pg:
            self.job_params = json.load(pg)
        with open(recs_file, "r") as pg:
            self.recs = json.load(pg)
        with open(best_fit_file, "r") as pg:
            self.best_fit_recs = json.load(pg)

        self.recs_data = []
        self.recs_data.append(self.recs)

        self.best_fit_data = []
        self.best_fit_data.append(self.best_fit_recs)

        self.datastore_refs = ['s3://s3-us-west-2.amazonaws.com:80/nisar-dev-lts-fwd-mkarim/products/POE/2018/06/03/NISAR_ANC_J_PR_POE_20180504T145019_20180603T000000_20180605T005942/NISAR_ANC_J_PR_POE_20180504T145019_20180603T000000_20180605T005942.xml']

    def tearDown(self):
        umock.patch.stopall()

    def test_success(self):
        import opera_chimera.precondition_functions

        recs2check_mock = umock.patch("opera_chimera.precondition_functions.ancillary_es.perform_aggregate_range_intersection_query").start()
        recs2check_mock.return_value = self.recs_data

        best_fit_mock = umock.patch("opera_chimera.precondition_functions.ancillary_es.select_best_fit").start()
        best_fit_mock.return_value = self.best_fit_data

        datastore_refs_mock = umock.patch("opera_chimera.precondition_functions.ancillary_es.get_datastore_refs_from_es_records").start()
        datastore_refs_mock.return_value = self.datastore_refs

        self.pf = opera_chimera.precondition_functions.OperaPreConditionFunctions(self.context, self.pge_config, self.settings, self.job_params)

        result = self.pf.get_orbit_ephemeris()

        print("result : {}".format(result))
        assert result['OrbitEphemerisFile'] == self.datastore_refs

    def test_failure(self):
        import opera_chimera.precondition_functions

        self.job_params["RangeStartDateTime"] = "2020-06-04T00:10:10.000000Z"
        self.job_params["RangeStopDateTime"] = "2020-06-04T00:40:42.000000Z"
        exp_output = "Could not find any Orbit Ephemeris files of type(s) ['POE', 'MOE', 'NOE', 'FOE'] over 2020-06-03T23:10:10.000000Z to 2020-06-04T01:40:42.000000Z"

        recs2check_mock = umock.patch("opera_chimera.precondition_functions.ancillary_es.perform_aggregate_range_intersection_query").start()
        recs2check_mock.side_effect = ValueError(exp_output)

        self.pf = opera_chimera.precondition_functions.OperaPreConditionFunctions(self.context, self.pge_config, self.settings, self.job_params)
        try:
            self.pf.get_orbit_ephemeris()
        except Exception as err:
            self.assertTrue(str(err).startswith(exp_output))

        self.assertRaises(Exception, self.pf, exp_output)

    def test_OE_TYPE_check(self):
        import opera_chimera.precondition_functions

        self.pge_config["get_orbit_ephemeris"]["type"] = "NOE"
        exp_output = "Could not find any Orbit Ephemeris files of type(s) ['NOE'] over 2018-06-03T23:10:10.000000Z to 2018-06-04T01:40:42.000000Z"

        recs2check_mock = umock.patch("opera_chimera.precondition_functions.ancillary_es.perform_aggregate_range_intersection_query").start()
        recs2check_mock.side_effect = ValueError(exp_output)

        self.pf = opera_chimera.precondition_functions.OperaPreConditionFunctions(self.context, self.pge_config, self.settings, self.job_params)
        try:
            self.pf.get_orbit_ephemeris()
        except Exception as err:
            self.assertTrue(str(err).startswith(exp_output))

        self.assertRaises(Exception, self.pf, exp_output)


if __name__ == "__main__":
    unittest.main()
