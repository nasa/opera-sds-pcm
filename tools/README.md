# OPERA SDS PCM Tools

# Audit

The audit tool can be used to compare input products and output product quantities.
See `audit.py --help`, documentation comments, and source code for more details.

## Getting Started

### Prerequisites

1. Git.
2. Python (see `.python-version`).
4. A clone of the `opera-sds-pcm` repo.

### Installation

1. Create a python virtual environment.
    1. RECOMMENDED: move `pip.conf` into the resulting `venv/` directory.
2. Activate the virtual environment and install the script dependencies referenced in the imports section as needed.

### Running locally

1. Configure `.env` as needed.
1. Run `python audit.py` from the same directory.

# CMR Audit

The CMR audit tool can be used to compare input products and output product IDs and quantities.
See `cmr_audit.py --help`, documentation comments, and source code for more details.

## Getting Started

### Prerequisites

1. Git.
1. Python (see `.python-version`).
1. A clone of the `opera-sds-pcm` repo.

### Installation

1. Create a python virtual environment.
    1. RECOMMENDED: move `pip.conf` into the resulting `venv/` directory.
2. Activate the virtual environment and install the script dependencies referenced in the imports section as needed.

### Running locally

1. Run `python cmr_audit.py` from the same directory.
