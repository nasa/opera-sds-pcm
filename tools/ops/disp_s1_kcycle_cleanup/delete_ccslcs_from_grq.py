#!/usr/bin/env python3
"""Delete CCSLC products from GRQ Elasticsearch in bulk, based on cleanup_manifest.json.

Reads `ccslc_grq_products` from each frame in the manifest and issues
Elasticsearch _bulk delete requests in configurable chunks.

Intended to run on the mozart instance where GRQ ES is reachable directly.

Usage:
    python delete_ccslcs_from_grq.py [--manifest cleanup_manifest.json]
                                     [--grq-url http://localhost:9200]
                                     [--chunk-size 1000]
                                     [--dry-run]

Examples:
    # Dry-run (count only, no requests sent)
    python delete_ccslcs_from_grq.py --dry-run

    # Actually delete, using default GRQ on localhost
    python delete_ccslcs_from_grq.py

    # Custom GRQ endpoint
    python delete_ccslcs_from_grq.py --grq-url http://100.104.42.50:9200
"""

import argparse
import json
import sys
import time
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


def chunked(seq, size):
    """Yield successive chunks of `size` from `seq`."""
    chunk = []
    for item in seq:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def iter_ccslcs(manifest_path):
    """Yield (index, id) tuples for every CCSLC in the manifest."""
    with open(manifest_path) as f:
        manifest = json.load(f)
    for frame in manifest:
        for p in frame.get("ccslc_grq_products", []):
            yield p["index"], p["id"]


def build_bulk_body(chunk):
    """Build a newline-delimited JSON body for _bulk delete."""
    lines = []
    for index, doc_id in chunk:
        lines.append(json.dumps({"delete": {"_index": index, "_id": doc_id}}))
    return "\n".join(lines) + "\n"


def post_bulk(grq_url, body):
    """POST a bulk body to ES _bulk and return the parsed response."""
    url = f"{grq_url.rstrip('/')}/_bulk"
    req = Request(
        url,
        data=body.encode("utf-8"),
        headers={"Content-Type": "application/x-ndjson"},
        method="POST",
    )
    with urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--manifest", type=Path,
                        default=Path(__file__).parent / "cleanup_manifest.json",
                        help="Path to cleanup_manifest.json (default: alongside this script)")
    parser.add_argument("--grq-url", default="http://localhost:9200",
                        help="GRQ Elasticsearch URL (default: http://localhost:9200)")
    parser.add_argument("--chunk-size", type=int, default=1000,
                        help="Number of deletes per _bulk request (default: 1000)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't send requests; just count and preview")
    args = parser.parse_args()

    if not args.manifest.exists():
        print(f"ERROR: manifest not found: {args.manifest}", file=sys.stderr)
        sys.exit(1)

    # Connectivity probe (skip on dry-run)
    if not args.dry_run:
        try:
            urlopen(f"{args.grq_url.rstrip('/')}/_cluster/health", timeout=10)
        except (URLError, HTTPError) as e:
            print(f"ERROR: cannot reach GRQ at {args.grq_url}: {e}", file=sys.stderr)
            sys.exit(1)

    ccslcs = list(iter_ccslcs(args.manifest))
    total = len(ccslcs)
    print(f"Loaded {total} CCSLC delete actions from {args.manifest}")
    print(f"GRQ URL:    {args.grq_url}")
    print(f"Chunk size: {args.chunk_size}")
    print(f"Mode:       {'DRY RUN' if args.dry_run else 'LIVE DELETE'}")

    if args.dry_run:
        print("\nFirst 5 actions:")
        for index, doc_id in ccslcs[:5]:
            print(f"  DELETE /{index}/_doc/{doc_id}")
        print(f"\n... {total} total deletes would be sent in "
              f"{(total + args.chunk_size - 1) // args.chunk_size} chunks.")
        return

    print()
    start = time.time()
    deleted = 0
    not_found = 0
    errors = 0
    error_samples = []

    for i, chunk in enumerate(chunked(ccslcs, args.chunk_size), start=1):
        body = build_bulk_body(chunk)
        t0 = time.time()
        try:
            result = post_bulk(args.grq_url, body)
        except (URLError, HTTPError) as e:
            print(f"  Chunk {i}: HTTP error — {e}", file=sys.stderr)
            errors += len(chunk)
            error_samples.append(str(e))
            continue
        elapsed = time.time() - t0

        chunk_deleted = chunk_nf = chunk_err = 0
        for item in result.get("items", []):
            d = item.get("delete", {})
            r = d.get("result")
            status = d.get("status", 0)
            if r == "deleted":
                chunk_deleted += 1
            elif r == "not_found" or status == 404:
                chunk_nf += 1
            elif status >= 400:
                chunk_err += 1
                if len(error_samples) < 5:
                    error_samples.append(json.dumps(d))

        deleted += chunk_deleted
        not_found += chunk_nf
        errors += chunk_err

        print(f"  Chunk {i:>3}: {len(chunk):>5} ops in {elapsed:5.2f}s | "
              f"deleted={chunk_deleted}, not_found={chunk_nf}, errors={chunk_err}")

    total_elapsed = time.time() - start

    print(f"\n{'='*60}")
    print(f"Summary — {total_elapsed:.1f}s total")
    print(f"{'='*60}")
    print(f"  Submitted:   {total}")
    print(f"  Deleted:     {deleted}")
    print(f"  Not found:   {not_found}  (already gone — fine)")
    print(f"  Errors:      {errors}")
    if error_samples:
        print(f"\n  First error samples:")
        for s in error_samples[:5]:
            print(f"    {s}")

    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
