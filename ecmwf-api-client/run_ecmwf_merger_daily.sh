#!/bin/bash

# === Config ===
EMAIL="Hyun.Lee@jpl.nasa.gov"
SETTINGS_FILE="$HOME/verdi/etc/settings.yaml"
TARGET_DATE=$(date -d "3 days ago" +%F)
END_DATE=$(date -d "$TARGET_DATE +1 day" +%F)

# === Extract RELEASE_VERSION from YAML ===
RELEASE=$(grep "^RELEASE_VERSION" "$SETTINGS_FILE" | awk -F"'" '{print $2}')

# === Run the merger ===
MERGER_SCRIPT="$HOME/mozart/ops/opera-pcm/ecmwf-api-client/run_merger_by_range_linux.sh"

if ! sh "$MERGER_SCRIPT" \
  --bucket=opera-ecmwf \
  --target-bucket=opera-ecmwf \
  --release="$RELEASE" \
  --start-date="$TARGET_DATE" \
  --end-date="$END_DATE"; then
    echo "Merger job failed on $(date) for date range $TARGET_DATE to $END_DATE with release=$RELEASE" | \
    mail -s "❌ ECMWF Merger Job Failed" "$EMAIL"
fi
