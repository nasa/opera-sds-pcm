# Contributing to OPERA SDS PCM

## Setting up a development environment

>All example commands should be executed from the project root directory unless indicated otherwise.

### Prerequisites

1. Git.
2. Python (see `.python-version`).
3. A clone of the `opera-sds-pcm` repo.

### Setup

1. Create a python virtual environment named `venv`.
    1. RECOMMENDED: move `pip.conf` into the resulting `venv/` directory.
2. Activate the virtual environment and install dependencies listed in the relevant section of `setup.py` using a command like the following `pip install -e '.[test]'`

## Running Tests

To run the tests:

Install test dependencies
```shell
pip install -e '.[test]'
```

Run unit tests
```shell
pytest

# or this
pytest tests/unit
```

There are also higher-level tests to cover system behaviors and workflows, located under `tests/integration` and other directories under `tests`

By default, `pytest` has been configured to run only unit tests under `tests/unit`
