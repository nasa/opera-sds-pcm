#!/bin/bash
source $HOME/verdi/bin/activate
export PYTHONPATH=.:$PYTHONPATH

# run unit tests
pytest --junit-xml=/tmp/pytest_unit.xml -o junit_family=xunit1

# run linting and pep8 style check (configured by ../.flake8)
flake8 --output-file=/tmp/flake8.log

# run code coverage
pytest --cov . --cov-report=html:/tmp/coverage.html

exit 0
