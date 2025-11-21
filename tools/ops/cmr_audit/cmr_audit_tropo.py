import requests
from datetime import datetime, timedelta
from collections import defaultdict
import csv
import sys
import argparse

def query_cmr_granules_all(provider, short_name, start_date_str, end_date_str, page_size=2000):
    base_url = 'https://cmr.earthdata.nasa.gov/search/granules.umm_json'
    page_num = 1
    all_entries = []

    while True:
        query_params = {
            'provider': provider,
            'short_name': short_name,
            'temporal': f'{start_date_str}T00:00:01Z,{end_date_str}T23:59:59Z',
            'page_size': page_size,
            'page_num': page_num
        }

        response = requests.get(base_url, params=query_params)
        response.raise_for_status()

        data = response.json()
        entries = data.get('items', [])

        if not entries:
            break

        all_entries.extend(entries)
        print(f"Fetched page {page_num} with {len(entries)} entries...")

        page_num += 1

    return all_entries

def daterange(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)

def count_granules_by_beginning_date(entries, start_date, end_date):
    counts = defaultdict(int)
    for entry in entries:
        umm = entry.get('umm', {})
        temporal = umm.get('TemporalExtent', {}).get('RangeDateTime', {})
        begin_dt = temporal.get('BeginningDateTime', None)

        if begin_dt:
            date_str = begin_dt.split('T')[0]
            counts[date_str] += 1

    for single_date in daterange(start_date, end_date):
        date_str = single_date.strftime('%Y-%m-%d')
        if date_str not in counts:
            counts[date_str] = 0

    return counts

def export_counts_to_csv(counts, filename='tropo_granule_counts.csv'):
    with open(filename, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Date', 'GranuleCount'])
        for date in sorted(counts.keys()):
            writer.writerow([date, counts[date]])
    print(f"Counts exported to {filename}")

def export_missing_dates_to_csv(counts, filename='tropo_missing_dates.csv'):
    missing_dates = [date for date, count in counts.items() if count < 4]
    with open(filename, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Date'])
        for date in sorted(missing_dates):
            writer.writerow([date])
    print(f"Missing dates exported to {filename}")

def parse_args():
    parser = argparse.ArgumentParser(
        description="Query CMR for OPERA Tropo granule counts and generate accountability reports",
        epilog="""
Examples:
  # Use default date range (2016-01-01 to today - 2 days):
  python cmr_audit_tropo.py --start 2016-01-01

  # Query specific date range:
  python cmr_audit_tropo.py --start 2016-01-01 --end 2016-06-30

  # Query with custom provider and product:
  python cmr_audit_tropo.py --start 2016-01-01 --provider ASF --short_name OPERA_L4_TROPO-ZENITH_V1

This script generates two CSV files:
  - tropo_granule_counts.csv: Count of granules for each date
  - tropo_missing_dates.csv: Dates with fewer than 4 granules
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--start', type=str, default="2016-01-01",
        help="Start date in YYYY-MM-DD format (default: 2016-01-01)"
    )
    parser.add_argument(
        '--end', type=str,
        help="End date in YYYY-MM-DD format (default: today - 2 days)"
    )
    parser.add_argument(
        '--provider', type=str, default='ASF',
        help="CMR data provider (default: ASF)"
    )
    parser.add_argument(
        '--short_name', type=str, default='OPERA_L4_TROPO-ZENITH_V1',
        help="Granule short name (default: OPERA_L4_TROPO-ZENITH_V1)"
    )

    # Show help if no arguments are provided
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    args = parser.parse_args()

    # Parse start date
    try:
        start_date = datetime.strptime(args.start, '%Y-%m-%d')
    except ValueError:
        print("Error: Invalid start date format. Use YYYY-MM-DD.")
        sys.exit(1)

    # Parse end date
    if args.end:
        try:
            end_date = datetime.strptime(args.end, '%Y-%m-%d')
        except ValueError:
            print("Error: Invalid end date format. Use YYYY-MM-DD.")
            sys.exit(1)
    else:
        end_date = datetime.utcnow() - timedelta(days=2)

    if start_date > end_date:
        print("Error: Start date cannot be after end date.")
        sys.exit(1)

    return args.provider, args.short_name, start_date, end_date

if __name__ == "__main__":
    provider, short_name, start_date, end_date = parse_args()

    entries = query_cmr_granules_all(
        provider,
        short_name,
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d')
    )

    counts = count_granules_by_beginning_date(entries, start_date, end_date)

    print("\nGranule counts by BeginningDateTime:")
    for date in sorted(counts.keys()):
        print(f"{date}: {counts[date]}")

    export_counts_to_csv(counts)
    export_missing_dates_to_csv(counts)

