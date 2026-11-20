# AGENTS.md

## Project overview

OPERA PCM (Process Control & data Management) orchestrates NASA's OPERA Science Data System. It queries NASA CMR and ESA Copernicus Dataspace for satellite input data, catalogs and downloads it, decides when inputs are complete enough to process, runs OPERA PGEs (Product Generation Executables), and delivers products to DAACs.

It is an *adaptation* of HySDS: this repo supplies OPERA-specific logic, job definitions, and config. The HySDS core (`hysds`, `hysds_commons`, `chimera`, `osaka`, `prov_es`) and `pcm_commons` are pinned dependencies (see `setup.py` `[test]` extras).

Python 3.12. pip only.

## Build and test commands

```bash
# Unit tests. pytest.ini sets testpaths=tests/unit, so use bare `pytest`, NOT `pytest tests/`
pytest
pytest tests/unit/data_subscriber/test_cmr_query.py::test_name
pytest -m "not cmr"        # skip tests that hit real CMR (needs network)
pytest --no-cov            # only if coverage addopts are re-enabled; pytest-cov breaks debuggers

# Integration/benchmark: need a deployed cluster + .env in the cwd (see tests/integration/README.md)
pip install -e '.[integration]'
cd tests && pytest integration/

./docker/build-container.sh <repo> <tag>   # what CI does: validates docker/*.json pairs, builds docker/Dockerfile
```

`setup.py` has narrow extras so subsystems install standalone. Install the one matching what you're touching.

Developers run `terraform plan` / `terraform apply` from `cluster_provisioning/dev/` for their personal dev cluster. Shared Terraform logic is in `cluster_provisioning/modules/common/`.

## Known pitfalls

- **Dependency pins are load-bearing.** `elasticsearch>=7.0.0,<7.14.0` and `numpy<2.0.0` are repeated across extras because ES 7.13.4 imports `np.float_`, removed in numpy 2.0. Don't relax them. GDAL is not in `setup.py`; install natively (`brew install gdal`). Unit tests stub `osgeo` when absent.
- **CI does not gate on tests.** `docker/run_tests.sh` ends pytest and flake8 with `|| true` (OPERA-2294). Green CI does not mean the suite passed; run `pytest` locally. CI is Jenkins, not GitHub Actions.
- **Figaro truncates error messages to 35 chars.** Operators group failed jobs by these strings. Raise a short, stable `RuntimeError("...")`, put detail in `err.add_note(...)`, chain with `raise ... from e`. Never swallow errors silently. Script entry points use `@exec_wrapper` (`util/exec_util.py`).
- **Ops tools that run on Mozart** (e.g. `tools/delete_disp_s1_large_gap.py`) intentionally import nothing from opera_pcm so they work with Mozart's Python. Keep them that way.

## Testing

- `tests/unit/conftest.py` pre-registers fakes in `sys.modules` before real code is imported. An autouse `deny_network_requests` fixture breaks all outbound HTTP.
- ES is mocked with `MockElasticsearchUtility` in conftest. CMR is mocked by patching `data_subscriber.cmr.async_cmr_posts` with `AsyncMock`.
- Only marker: `cmr` (real CMR, network), applied per test. `asyncio_mode = strict`, so async tests need `@pytest.mark.asyncio`.
- Write plain pytest functions with sentence-style names (`test_cslc_catalog_ingest_runs_on_the_cslc_download_workers`). Don't convert existing `TestCase` classes. Use `parametrize` with readable `ids=`. Larger fixtures go in `tests/unit/<pkg>/test_data/`.

## Architecture

Two mostly independent pipelines, joined by the Mozart job queue and the GRQ ES/OpenSearch catalogs.

### Data acquisition: `data_subscriber/`

`daac_data_subscriber.py` is the single CLI (`query` / `download` / `full` / `survey`) every per-product job script invokes. Args are in `parser.py`.

One class triple per product: `BaseQuery` (`query.py`: query → dedupe → completeness check → catalog → submit downloads), `ProductCatalog` (`catalog.py`, ES doc read/write; `BulkCatalog` for HLS/SLC), `BaseDownload` (`download.py`: fetch, then `extractor.extract`).

**`cmr.py` is the dispatch table.** No factory: `Collection`/`Provider`/`ProductType`/`PGEProduct` enums plus `COLLECTION_TO_*_MAP` dicts drive if/elif chains in `daac_data_subscriber.run_query/run_download/supply_es_conn`. Adding a product means touching those maps *and* chains. `PGEProduct.DIST_1` disambiguates RTC feeding DSWx-S1 vs DIST-S1.

Completeness logic lives in `determine_download_granules()` overrides and evaluators (`rtc/evaluator.py`, `rtc_for_dist/rtc_batch_evaluator.py`, `cslc_utils.py`). Jobs with unmet dependencies (compressed CSLC for DISP-S1, previous tile for DIST-S1) are **parked** as pending-job ES docs; `submit_pending_jobs.py` and `dist_s1/submitter_forward.py` promote them later.

### PGE execution: Chimera + SciFlo

A `job-SCIFLO_<PGE>` job runs `opera_chimera/run_sciflo.sh` on `opera_chimera/wf_xml/<PGE>.sf.xml`. Its key steps (classes bound in `opera_chimera/configs/chimera_config.yaml`):

1. **Preprocessor**: `opera_chimera/precondition_functions.py::OperaPreConditionFunctions` runs the ordered `preconditions:` list from `configs/pge_configs/PGE_<PGE>.yaml`. Each returns a dict merged into job params that fill `__CHIMERA_VAL__` placeholders. **Order matters.** Most PGE-specific input logic lives here.
2. **PGE job**: `opera_pge_job_submitter.py::OperaPgeJobSubmitter` downloads inputs, then calls `wrapper/opera_pge_wrapper.py::run_pipeline()` **in-process**, which renders the RunConfig and runs the PGE container.

`PGE_SIMULATION_MODE` in `conf/settings.yaml` skips the PGE container; `util/pge_util.py::simulate_run_pge` fabricates outputs.

**Adding/changing a PGE touches, in lockstep:** `wf_xml/<PGE>.sf.xml`, `pge_configs/PGE_<PGE>.yaml`, `conf/RunConfig.yaml.<PGE>.jinja2.tmpl` + `conf/schema/` schema, `conf/pge_outputs.yaml`, `PRODUCT_TYPES` in `conf/settings.yaml`, precondition methods, and the `docker/hysds-io.json.SCIFLO_<PGE>` / `job-spec.json.SCIFLO_<PGE>` pair.

`conf/sds/rules/user_rules*.json*` are the HySDS **trigger rules** that chain query → download → SCIFLO.

## Code style guidelines

No formatter or lint config is enforced. **The surrounding file wins.**

- **Never reformat code you aren't changing.** No black/ruff/isort exists; reformats bury the real diff.
- Annotate signatures of new functions; prefer `list[str]`, `X | None`. Avoid `Any`; use precise types or `object`. Don't add hints to untouched code.
- Enumerated strings are `StrEnum`. Metadata keys come from `opera_commons/constants/product_metadata.py`, not inline strings.
- TODOs carry a ticket: `# TODO(OPERA-1234): ...`.

## Commit and PR guidelines

- Branch = Jira ticket (`OPERA-1234`). PRs target `develop` and follow `.github/PULL_REQUEST_TEMPLATE.md`.
- Commit subject: `OPERA-1234: <sentence describing resulting behavior>`. Multiple tickets: `OPERA-1234, OPERA-5678: ...`. Untracked work: `OPERA-0000:`.

## Glossary

- **Burst**: S1 IW burst, ID like `T168-359595-IW3`.
- **Frame**: fixed group of bursts; DISP-S1's processing unit.
- **MGRS tile**: DSWx/DIST output grid.
- **k-cycle**: DISP-S1 processes a frame's dates in groups of k=15; each writes a compressed CSLC (CCSLC) seeding the next group.
- **CBDB**: consistent burst DB driving DISP-S1 historical processing.
- **Forward vs historical**: triggered-on-arrival vs campaign over `batch_proc` ES docs.
- **GRQ**: product catalog (ES/OpenSearch)
- **Mozart/Figaro**: job orchestration and its UI.
- **Verdi**: worker-side job agent.
- **PGE**: the wrapper container (built in `opera-sds-pge`).
- **SAS**: the science algorithm inside the PGE. "SAS failure" means the algorithm failed, not PCM.
