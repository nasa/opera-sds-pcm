# Contributing to OPERA SDS PCM

## Setting up a development environment

>All example commands should be executed from the project root directory unless indicated otherwise.

## Running Tests

To run the tests:

Install test dependencies
```shell
pip install '.[test]'
```

Run unit tests
```shell
pytest

# or this
pytest tests/unit
```

There are also higher-level tests to cover system behaviors and workflows, located under `tests/integration` and other directories under `tests`

By default, `pytest` has been configured to run only unit tests under `tests/unit`
