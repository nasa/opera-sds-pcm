#!/usr/bin/env bash

#######################################################################
# This script is a wrapper for running opera unit tests.
#######################################################################


######################################################################
# Main script body
######################################################################

echo Executing unit tests. This should be quick...

python -m venv venv
source venv/bin/activate

echo Copying ancillary files from dev s3...
aws s3 cp s3://opera-ancillaries/nevada_opera.geojson .
aws s3 cp s3://opera-ancillaries/california_opera.geojson .
aws s3 cp s3://opera-ancillaries/north_america_opera.geojson .

pytest --maxfail=2 \
  tests/data_subscriber/test_query.py \
  tests/data_subscriber/test_cslc_query.py
set -e

echo Copying JUnit report to public directory for CI/CD integration
cp -f target/reports/junit/junit.xml /tmp/junit.xml
cp -f target/reports/junit/junit.xml /export/home/hysdsops/junit.xml

echo Cleaning up...
rm nevada_opera.geojson
rm california_opera.geojson
rm north_america_opera.geojson