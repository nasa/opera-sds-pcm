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

1. Create a python virtual environment.
    1. RECOMMENDED: move `pip.conf` into the resulting `venv/` directory.
2. Activate the virtual environment and install the script dependencies referenced in the imports section as needed.
    1. RECOMMENDED: install dependencies listed in the relevant section of `setup.py` using the following command `pip install '.[audit]' && pip uninstall $(python setup.py --name) --yes'`

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

### Installation

1. Create a python virtual environment.
    1. RECOMMENDED: move `pip.conf` into the resulting `venv/` directory.
2. Activate the virtual environment and install the script dependencies referenced in the imports section as needed.
    1. RECOMMENDED: install dependencies listed in the relevant section of `setup.py` using the following command `pip install '.[cmr_audit]' && pip uninstall $(python setup.py --name) --yes'`

### Running locally

1. Run `python cmr_audit*.py` from the same directory.


# Elasticsearch Query Executor

The Elasticsearch Query Executor script is a script for executing version-controlled Elasticsearch queries, including for clean-up purposes.

Please see the `README.md` located in `es_query_executor/README.md` for more information.

