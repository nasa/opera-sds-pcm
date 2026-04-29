# OPERA SDS PCM

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/badge/Python-3.9-blue.svg)](https://www.python.org/downloads/)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](CODE_OF_CONDUCT.md)

**Process Control and Data Management (PCM)** system for the [Observational Products for End-Users from Remote Sensing Analysis (OPERA)](https://www.jpl.nasa.gov/go/opera) project. This is the core Science Data System (SDS) software that orchestrates end-to-end processing pipelines for generating satellite-derived geophysical products at NASA's Jet Propulsion Laboratory.

---

## Table of Contents

- [Overview](#overview)
- [Products](#products)
- [Architecture](#architecture)
- [Repository Structure](#repository-structure)
- [Getting Started](#getting-started)
- [Testing](#testing)
- [Cluster Provisioning](#cluster-provisioning)
- [Contributing](#contributing)
- [License](#license)

## Overview

OPERA PCM handles the full lifecycle of satellite data processing:

- **Data Discovery & Subscription** — Automated queries to NASA's [Common Metadata Repository (CMR)](https://cmr.earthdata.nasa.gov/), the Alaska Satellite Facility (ASF), and ESA Copernicus Data Space for input granule discovery.
- **Job Orchestration** — Processing pipeline management through [HySDS](https://github.com/hysds) (Hybrid Science Data System) and its [Chimera](https://github.com/hysds/chimera) framework.
- **Product Generation** — Execution of Product Generation Executables (PGEs) for each science product via containerized workflows.
- **Infrastructure Management** — AWS cluster provisioning and scaling using Terraform.
- **Product Delivery** — Cataloging and delivery of generated products to NASA DAACs via CNM (Cumulus Notification Messages).

## Products

OPERA PCM generates the following science data products:

| Product | Full Name | Source | Level |
|---------|-----------|--------|-------|
| **CSLC-S1** | Co-registered Single Look Complex | Sentinel-1 | L2 |
| **RTC-S1** | Radiometric Terrain Corrected | Sentinel-1 | L2 |
| **DISP-S1** | Surface Displacement | Sentinel-1 | L3 |
| **DIST-S1** | Surface Disturbance | Sentinel-1 | L3 |
| **DSWx-HLS** | Dynamic Surface Water Extent | Harmonized Landsat Sentinel-2 | L3 |
| **DSWx-S1** | Dynamic Surface Water Extent | Sentinel-1 | L3 |
| **DSWx-NI** | Dynamic Surface Water Extent | NISAR | L3 |
| **DISP-NI** | Surface Displacement | NISAR | L3 |
| **TROPO** | Tropospheric Delay Corrections | ECMWF | L4 |
| **CAL-DISP** | Calibrated Displacement | — | L4 |

Products are distributed through NASA's [Earthdata](https://www.earthdata.nasa.gov/) platform via the LP DAAC and ASF DAAC.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        OPERA PCM System                         │
├──────────────┬──────────────┬──────────────┬────────────────────┤
│              │              │              │                    │
│  Data        │  Chimera     │  PGE         │  Product           │
│  Subscriber  │  Orchestrator│  Wrapper     │  Delivery          │
│              │              │              │                    │
│  ┌────────┐  │  ┌────────┐  │  ┌────────┐  │  ┌──────────────┐ │
│  │ CMR    │  │  │Precon- │  │  │Docker  │  │  │ CNM Response │ │
│  │ Query  │  │  │dition  │  │  │Contain-│  │  │ Processing   │ │
│  │        │  │  │Check   │  │  │erized  │  │  │              │ │
│  │ ASF    │  │  │        │  │  │PGE     │  │  │ DAAC         │ │
│  │ Download│  │  │Job     │  │  │Execu-  │  │  │ Cataloging   │ │
│  │        │  │  │Submit  │  │  │tion    │  │  │              │ │
│  │ DAAC   │  │  │        │  │  │        │  │  │ Elasticsearch│ │
│  │ Ingest │  │  │Post-   │  │  │Metrics │  │  │ Indexing     │ │
│  │        │  │  │process │  │  │Collect │  │  │              │ │
│  └────────┘  │  └────────┘  │  └────────┘  │  └──────────────┘ │
└──────────────┴──────────────┴──────────────┴────────────────────┘
        │              │              │                │
        ▼              ▼              ▼                ▼
   NASA CMR       HySDS/Mozart    Docker         LP DAAC / ASF
   ASF / ESA      Elasticsearch   Registry       Earthdata
```

### Key Components

- **Data Subscriber** (`data_subscriber/`) — Queries CMR, ASF, and ESA Copernicus for input data. Handles granule filtering, download scheduling, and CSLC/RTC/HLS-specific logic.
- **Chimera Orchestrator** (`opera_chimera/`) — Configures processing pipelines: precondition validation, PGE job submission, and post-processing. Built on the [HySDS Chimera](https://github.com/hysds/chimera) framework.
- **PGE Wrapper** (`wrapper/`) — Thin wrapper around containerized Product Generation Executables. Manages RunConfig generation, execution, and output harvesting.
- **Cluster Provisioning** (`cluster_provisioning/`) — Terraform modules for deploying OPERA SDS clusters on AWS (dev, int, ops environments).
- **Airflow DAGs** (`airflow/`) — Workflow definitions for Apache Airflow-based pipeline orchestration.
- **Commons** (`opera_commons/`) — Shared utilities: Elasticsearch connections, logging, and constants.
- **Tools** (`tools/`) — Operational and analysis utilities: burst database tools, historical processing scripts, CMR auditing, deployment helpers.

## Repository Structure

```
opera-sds-pcm/
├── airflow/                  # Apache Airflow DAGs and Terraform configs
├── cluster_provisioning/     # Terraform modules for AWS deployment
│   ├── dev/                  #   Development environment
│   ├── int/                  #   Integration & test environment
│   ├── ops/                  #   Operations (production) environment
│   └── modules/              #   Shared Terraform modules
├── conf/                     # RunConfig and AlgoParams Jinja2 templates
├── data_subscriber/          # CMR/ASF/ESA data discovery and download
│   ├── cslc/                 #   CSLC-specific query logic
│   ├── hls/                  #   HLS-specific query logic
│   ├── rtc/                  #   RTC-specific query logic
│   └── slc/                  #   SLC-specific query logic
├── dist_s1/                  # DIST-S1 product-specific processing
├── docker/                   # Dockerfile and HySDS job/IO specs
├── extractor/                # Product metadata extraction
├── geo/                      # Geospatial utilities and GeoJSON data
├── job_accountability/       # Job tracking and accountability
├── opera_chimera/            # HySDS Chimera orchestration layer
│   ├── configs/              #   Chimera pipeline configuration
│   └── constants/            #   OPERA-specific constants
├── opera_commons/            # Shared libraries (logging, ES, constants)
├── product2dataset/          # Product-to-HySDS-dataset conversion
├── tests/                    # Test suite
│   ├── unit/                 #   Unit tests (default pytest target)
│   ├── integration/          #   Integration tests
│   ├── regression/           #   Regression tests
│   ├── benchmark/            #   Performance benchmarks
│   └── scenarios/            #   End-to-end scenario tests
├── tools/                    # Operational utilities and scripts
├── util/                     # General-purpose utilities
├── wrapper/                  # PGE execution wrapper
└── setup.py                  # Package configuration and dependencies
```

## Getting Started

### Prerequisites

- **Python 3.9** (see `.python-version`)
- **Git**
- Optionally: **GDAL** native libraries (`brew install gdal` on macOS, or via conda)

### Installation

```bash
# Clone the repository
git clone https://github.com/nasa/opera-sds-pcm.git
cd opera-sds-pcm

# Create and activate a virtual environment
python -m venv venv
cp pip.conf venv/          # Recommended: configure pip for JPL indexes
source venv/bin/activate   # On Windows: venv\Scripts\activate

# Install with test dependencies
pip install -e '.[test]'
```

> **Note:** Some dependencies (e.g., `prov-es`, `osaka`, `hysds`, `chimera`) are installed from HySDS GitHub releases. Ensure you have network access to GitHub during installation.

### Dependency Groups

The `setup.py` defines several extras for different use cases:

| Extra | Purpose |
|-------|---------|
| `test` | Local development and unit testing |
| `docker` | Dependencies bundled in the Docker image |
| `subscriber` | Standalone data subscriber execution |
| `integration` | Integration test suite |
| `benchmark` | Performance benchmarking |
| `audit` | Internal audit tools |
| `cmr_audit` | CMR audit and reconciliation tools |

Install a specific group with:

```bash
pip install -e '.[subscriber]'
```

## Testing

Unit tests are the default target and can be run without cloud credentials:

```bash
# Run all unit tests
pytest

# Run unit tests explicitly
pytest tests/unit

# Run with coverage
pytest --cov=data_subscriber --cov=opera_chimera tests/unit
```

Higher-level test suites require additional configuration:

```bash
# Integration tests (requires AWS credentials and cluster access)
pip install -e '.[integration]'
pytest tests/integration

# Benchmarks
pip install -e '.[benchmark]'
pytest tests/benchmark
```

See [TESTING.md](TESTING.md) for more details.

## Cluster Provisioning

OPERA PCM clusters are deployed on AWS using Terraform. See [cluster_provisioning/README.md](cluster_provisioning/README.md) for full instructions.

```bash
cd cluster_provisioning/dev   # or int/ or ops/
terraform init
terraform plan
terraform apply
```

Environments:
- **dev** — Development and feature testing
- **int** — Integration and system testing
- **ops/pst** — Operations and pre-production

## Contributing

We welcome contributions! To get started:

1. Fork the repository and create a feature branch from `develop`.
2. Set up your development environment following the [Getting Started](#getting-started) section.
3. Make your changes and ensure all unit tests pass (`pytest tests/unit`).
4. Submit a Pull Request following the [PR template](.github/PULL_REQUEST_TEMPLATE.md).

Please reference the relevant Jira ticket (OPERA-XXXX) in your PR when applicable.

> **Note:** This project tracks issues via [Jira](https://hysds-core.atlassian.net/) with OPERA-prefixed tickets. GitHub Issues are available for community bug reports and feature requests via the provided [issue templates](.github/ISSUE_TEMPLATE/).

## Related Projects

| Repository | Description |
|------------|-------------|
| [hysds/hysds](https://github.com/hysds/hysds) | Hybrid Science Data System — core job orchestration framework |
| [hysds/chimera](https://github.com/hysds/chimera) | PGE orchestration framework used by OPERA |
| [nasa/opera-sds-bach-api](https://github.com/nasa/opera-sds-bach-api) | OPERA Back-end API for the Accountability Subsystem |

## License

Copyright 2022-2024, California Institute of Technology. All rights reserved.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full text.

This software was developed at the Jet Propulsion Laboratory, California Institute of Technology, under a contract with the National Aeronautics and Space Administration (NASA). U.S. Government sponsorship acknowledged. Reference herein to any specific commercial product, process, or service does not constitute or imply its endorsement by the United States Government or the Jet Propulsion Laboratory, California Institute of Technology.
