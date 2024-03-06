# OPERA SDS PCM Tools

# Audit

The audit tools can be used to compare input products and output product quantities.
See `pcm_audit/*audit.py --help`, documentation comments, and source code for more details.

## Getting Started

### Prerequisites

1. Git.
2. Python (see `.python-version`).
3. A clone of the `opera-sds-pcm` repo.

### Installation

1. Create a python virtual environment named `venv`.
    1. RECOMMENDED: move `pip.conf` into the resulting `venv/` directory.
2. Activate the virtual environment and install the script dependencies referenced in the imports section as needed.
    1. RECOMMENDED: install dependencies listed in the relevant section of `setup.py` using the following command `pip install -e '.[audit]'`

### Running locally

1. Configure `.env` as needed.
1. Run `python *audit.py` from the same directory.

# CMR Audit

The CMR audit tool can be used to compare input products and output product IDs and quantities.
See `cmr_audit/cmr_audit.py --help`, documentation comments, and source code for more details.

## Getting Started

### Prerequisites

1. Git.
1. Python (see `.python-version`).
1. A clone of the `opera-sds-pcm` repo.
1. slc_audit.py requires GDAL to be natively installed on the host machine. Installation instructions vary by operating system, but macOS users with Homebrew installed may install it using `brew install gdal` or as otherwise noted in `setup.py` in the `cmr_audit` dependency section. NOTE: native `gdal` is distinct from the `GDAL` python package dependency.
1. slc_audit.py requires north_america_opera.geojson file to be present in the current working directory

### Installation

1. Create a python virtual environment named `venv`.
    1. RECOMMENDED: move `pip.conf` into the resulting `venv/` directory.
2. Activate the virtual environment and install the script dependencies referenced in the imports section as needed.
    1. RECOMMENDED: install dependencies listed in the relevant section of `setup.py` using the following command `pip install -e '.[cmr_audit]'`
2. If running slc_audit.py, place north_america_opera.geojson file in the current working directory. This file is found in the ancillary S3 and may also be in the opera-pcm repo.

### Running locally

1. Run `python cmr_audit*.py` from the same directory.


# Elasticsearch Query Executor

The Elasticsearch Query Executor script is a script for executing version-controlled Elasticsearch queries, including for clean-up purposes.

Please see the `README.md` located in `es_query_executor/README.md` for more information.


# Data Subscriber Client

The Data Subscriber Client tool can be used to reduce a given list of RTC products to those representing distinct MGRS set IDs.
See `data_subscriber/data_subscriber_client.py --help`, documentation comments, and source code for more details.

## Getting Started

### Prerequisites

1. Git.
1. Python (see `.python-version`).
1. A clone of the `opera-sds-pcm` repo.
1. data_subscriber_client.py requires GDAL to be natively installed on the host machine. Installation instructions vary by operating system, but macOS users with Homebrew installed may install it using `brew install gdal` or as otherwise noted in `setup.py` in the `cmr_audit` dependency section. NOTE: native `gdal` is distinct from the `GDAL` python package dependency.
1. data_subscriber_client.py requires the latest MGRS collection DB file (currently `MGRS_tile_collection_v0.3.sqlite`) to be present in `~/Downloads/` This file is found in the ancillary S3 and may also be in the opera-pcm repo.

### Installation

1. Create a python virtual environment named `venv`.
    1. RECOMMENDED: move `pip.conf` into the resulting `venv/` directory.
2. Activate the virtual environment and install the script dependencies referenced in the imports section as needed.
    1. RECOMMENDED: install dependencies listed in the relevant sections of `setup.py` using the following command `pip install -e '.[subscriber]' && pip install -e '.[test]' && pip install -e '.[subscriber_client]'`

### Running locally

1. Run `python data_subscriber/data_subscriber_client.py` from the same directory.
