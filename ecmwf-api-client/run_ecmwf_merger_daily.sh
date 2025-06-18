#!/bin/bash

# === Config ===
EMAIL="Hyun.Lee@jpl.nasa.gov"
SETTINGS_FILE=~/verdi/etc/settings.yaml
TARGET_DATE=$(date -d "3 days ago" +%F)

# === Extract RELEASE_VERSION from YAML ===
RELEASE=$(grep "^RELEASE_VERSION" "$SETTINGS_FILE" | awk -F"'" '{print $2}')

# === Run the merger ===
if ! sh ~/mozart/ops/opera-pcm/ecmwf-api-client/run_merger_by_range_linux.sh \
  --bucket=opera-ecmwf \
  --target-bucket=opera-ecmwf \
  --release="$RELEASE" \
  --start-date="$TARGET_DATE" \
  --end-date="$TARGET_DATE"; then
    echo "Merger job failed on $(date) for date $TARGET_DATE with release=$RELEASE" | \
    mail -s "❌ ECMWF Merger Job Failed" "$EMAIL"
fi
