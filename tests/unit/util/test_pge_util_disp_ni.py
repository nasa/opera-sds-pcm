"""Simulated DISP-NI PGE runs.

In simulation mode the DISP-NI smoke test walks frames through their k-sets without the PGE, so a
simulated run must output what the PGE would for the same inputs: as many products, named the same, and a
compressed GSLC the walker finds the frame's next k-set by. The expected names are those of a real run of
frame 26410's first k-set (PGE 6.0.0-rc.1.1), and of its second k-set as the disp-nisar SAS names it.
"""

import json

import h5py
import pytest

from util import pge_util

PGE_CONFIG = {
    "output_base_name": "OPERA_L3_DISP-NI_{mode}_{frame_id}_{pol}_{ref_datetime}Z_{sec_datetime}Z_{product_version}_"
                        "{creation_ts}Z",
    "ancillary_base_name": "OPERA_L3_DISP-NI_{mode}_{frame_id}_{pol}_{product_version}_{creation_ts}Z",
    "compressed_gslc_name": "OPERA_L2_COMPRESSED-GSLC-NI_{mode}_{frame_id}_{pol}_{ref_date}T000000Z_"
                            "{first_date}T000000Z_{last_date}T000000Z_{creation_ts}Z_{product_version}",
}

# Frame 26410 (NISAR 151_A_011): its first k-set, cycles 4 to 8, and its second, cycles 9 to 13
K_SET_1 = [(4, "20251108T155041"), (5, "20251120T155041"), (6, "20251202T155042"), (7, "20251214T155042"),
           (8, "20251226T155043")]
K_SET_2 = [(9, "20260107T155044"), (10, "20260119T155044"), (11, "20260131T155045"), (12, "20260212T155045"),
           (13, "20260224T155046")]


def gslc(cycle, start, mode_pol="4005_DHDH"):
    return f"NISAR_L2_PR_GSLC_{cycle:03d}_151_A_011_{mode_pol}_A_{start}_{start[:-2]}58_P05023_N_P_J_001.h5"


def gunw(reference, secondary, mode_pol="4000_SH"):
    (ref_cycle, ref_start), (sec_cycle, sec_start) = reference, secondary
    return (f"NISAR_L2_PR_GUNW_{ref_cycle:03d}_151_A_011_{sec_cycle:03d}_{mode_pol}_{ref_start}_{ref_start}_"
            f"{sec_start}_{sec_start}_P05023_N_P_J_001.h5")


def runconfig(k_set, compressed_gslcs=(), with_gunws=True, save_compressed_slc=True):
    """The runconfig the wrapper hands the PGE: inputs at their in-container paths, GSLCs first."""
    def in_container(name):
        return f"/home/mamba/input_dir/{name}"

    return {
        "input_file_group": {
            "input_file_paths": [in_container(gslc(*g)) for g in k_set] + [in_container(c) for c in compressed_gslcs],
            "compressed_gslc_paths": [f"s3://lts/{c}" for c in compressed_gslcs],
        },
        "dynamic_ancillary_file_group": {
            "gunw_files": [in_container(gunw(a, b)) for a, b in zip(k_set, k_set[1:])] if with_gunws else None,
        },
        "product_path_group": {"product_version": "0.1", "save_compressed_slc": save_compressed_slc},
        "processing": {"frame_id": 26410},
    }


@pytest.fixture
def job(tmp_path):
    """Runs the simulator as the wrapper does, in a job's work directory. Returns the outputs' names."""
    input_dir, output_dir = tmp_path / "pge_input_dir", tmp_path / "pge_output_dir"
    input_dir.mkdir()

    def run(config, inputs=()):
        for path in inputs:
            (input_dir / path.name).write_bytes(path.read_bytes())
        if output_dir.exists():
            for path in output_dir.iterdir():
                path.unlink()
        output_dir.mkdir(exist_ok=True)

        pge_util.simulate_run_pge(config, {"pge_name": "L3_DISP_NI", **PGE_CONFIG},
                                  {"job_specification": {"params": [{"name": "pge_input_dir",
                                                                     "value": "pge_input_dir"}]}},
                                  str(output_dir))
        return sorted(p.name for p in output_dir.iterdir())

    run.output_dir = output_dir
    return run


@pytest.fixture
def ts():
    return pge_util.get_time_for_filename()


def products(names):
    return [n.removesuffix(".nc") for n in names if n.endswith(".nc")]


def test_the_first_k_set_outputs_what_the_pge_did(job, ts):
    names = job(runconfig(K_SET_1))

    stems = [f"OPERA_L3_DISP-NI_4000_26410_SH_20251108T155041Z_{sec}Z_v0.1_{ts}Z"
             for sec in ("20251120T155041", "20251202T155042", "20251214T155042", "20251226T155043")]
    expected = [f"{stem}{suffix}" for stem in stems for suffix in (".nc", ".iso.xml", "_BROWSE.png")] + [
        f"OPERA_L2_COMPRESSED-GSLC-NI_4000_26410_SH_20251226T000000Z_20251108T000000Z_20251226T000000Z_{ts}Z_v1.0.h5",
        f"OPERA_L3_DISP-NI_4000_26410_SH_v0.1_{ts}Z.catalog.json",
        f"OPERA_L3_DISP-NI_4000_26410_SH_v0.1_{ts}Z.log",
    ]
    assert names == sorted(expected)
    catalog = json.loads((job.output_dir / f"OPERA_L3_DISP-NI_4000_26410_SH_v0.1_{ts}Z.catalog.json").read_text())
    assert set(catalog) == {"PGE_Version", "SAS_Version"}


def test_a_later_k_set_is_referenced_to_the_compressed_gslc_of_the_k_set_before_it(job, ts, tmp_path):
    job(runconfig(K_SET_1))
    k_set_1_compressed = next(job.output_dir.glob("*.h5"))
    kept = tmp_path / k_set_1_compressed.name
    kept.write_bytes(k_set_1_compressed.read_bytes())

    names = job(runconfig(K_SET_2, compressed_gslcs=[kept.name]), inputs=[kept])

    # Referenced to the start time of k-set 1's last GSLC, with a product for each of its own 5 GSLCs
    assert products(names) == [f"OPERA_L3_DISP-NI_4000_26410_SH_20251226T155043Z_{start}Z_v0.1_{ts}Z"
                               for _, start in K_SET_2]
    assert f"OPERA_L2_COMPRESSED-GSLC-NI_4000_26410_SH_20260224T000000Z_20260107T000000Z_20260224T000000Z_" \
           f"{ts}Z_v1.0.h5" in names


def test_with_several_compressed_gslcs_the_latest_is_the_reference(job, tmp_path):
    compressed = []
    for ref_date, start_time in (("20251226", "2025-12-26T15:50:43.000000"),
                                 ("20260224", "2026-02-24T15:50:46.123456789")):
        path = tmp_path / (f"OPERA_L2_COMPRESSED-GSLC-NI_4000_26410_SH_{ref_date}T000000Z_20251108T000000Z_"
                           f"{ref_date}T000000Z_20261028T153405Z_v1.0.h5")
        with h5py.File(path, "w") as f:
            f[pge_util.NISAR_ZERO_DOPPLER_START_TIME] = start_time
        compressed.append(path)
    k_set_3 = [(14, "20260308T155046"), (15, "20260320T155046"), (16, "20260401T155047"), (17, "20260413T155047"),
               (18, "20260425T155048")]

    # Listed oldest last, to show the order they are given in does not matter
    names = job(runconfig(k_set_3, compressed_gslcs=[c.name for c in reversed(compressed)]), inputs=compressed)

    assert {p.split("_")[6] for p in products(names)} == {"20260224T155046Z"}
    assert len(products(names)) == 5


def test_without_gunws_the_mode_and_polarization_come_from_the_gslcs(job):
    names = job(runconfig(K_SET_1, with_gunws=False))

    assert {n.split("_")[3:6] == ["4005", "26410", "DH"] for n in names if n.startswith("OPERA_L3")} == {True}
    assert len(products(names)) == 4


def test_no_compressed_gslc_is_output_unless_asked_for(job):
    names = job(runconfig(K_SET_1, save_compressed_slc=False))

    assert not [n for n in names if n.endswith(".h5")]
    assert len(products(names)) == 4


def test_a_compressed_gslc_without_a_start_time_is_taken_at_its_reference_date(job, tmp_path):
    compressed = tmp_path / ("OPERA_L2_COMPRESSED-GSLC-NI_4000_26410_SH_20251226T000000Z_20251108T000000Z_"
                             "20251226T000000Z_20261028T153405Z_v1.0.h5")
    compressed.write_bytes(b"not hdf5")

    names = job(runconfig(K_SET_2, compressed_gslcs=[compressed.name]), inputs=[compressed])

    assert {p.split("_")[6] for p in products(names)} == {"20251226T000000Z"}
