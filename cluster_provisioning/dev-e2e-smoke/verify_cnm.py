#!/usr/bin/env python
"""Verify CNM-S success and mock/verify CNM-R for smoke test products."""

import argparse
import json
import netrc
import sys
from pathlib import Path

import backoff
import boto3
from elasticsearch import Elasticsearch, NotFoundError


def get_es_client(host):
    http_auth = None
    netrc_path = Path("~/.netrc-os").expanduser()
    if netrc_path.exists():
        try:
            creds = netrc.netrc(str(netrc_path)).authenticators("default")
            if creds:
                http_auth = (creds[0], creds[2])
        except (netrc.NetrcParseError, OSError):
            pass
    return Elasticsearch(
        f"https://{host}:9200",
        http_auth=http_auth,
        verify_certs=False,
    )


@backoff.on_predicate(backoff.constant, interval=60, max_time=600)
def wait_for_cnm_s(es, index, product_id):
    """Wait for daac_CNM_S_status == SUCCESS on the product."""
    try:
        result = es.search(
            index=index,
            body={"query": {"match_phrase": {"id": product_id}}}
        )
        if result["hits"]["total"]["value"] == 0:
            return False
        doc = result["hits"]["hits"][0]["_source"]
        return doc.get("daac_CNM_S_status") == "SUCCESS"
    except NotFoundError:
        return False


def mock_cnm_r(sns_client, topic_arn, product_id):
    """Publish a mock CNM-R SUCCESS response to SNS."""
    response_body = {
        "version": "1.0",
        "provider": "JPL-OPERA",
        "collection": "OPERA_L3_DSWx-HLS",
        "submissionTime": "2022-01-01T12:00:00Z",
        "receivedTime": "2022-01-01T12:01:00Z",
        "processCompleteTime": "2022-01-01T12:05:00Z",
        "identifier": product_id,
        "response": {
            "status": "SUCCESS",
            "catalogId": "G1234567890-LPCLOUD",
            "catalogUrl": "https://cmr.earthdata.nasa.gov/search/concepts/G1234567890-LPCLOUD"
        }
    }
    sns_client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(response_body)
    )


@backoff.on_predicate(backoff.constant, interval=60, max_time=600)
def wait_for_cnm_r(es, index, product_id):
    """Wait for daac_delivery_status == SUCCESS after CNM-R mock."""
    try:
        result = es.search(
            index=index,
            body={"query": {"match_phrase": {"id": product_id}}}
        )
        if result["hits"]["total"]["value"] == 0:
            return False
        doc = result["hits"]["hits"][0]["_source"]
        return doc.get("daac_delivery_status") == "SUCCESS"
    except NotFoundError:
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--es-host", required=True)
    parser.add_argument("--cnm-r-topic-arn", required=True)
    parser.add_argument("--products", required=True, help="Comma-separated product ID prefixes")
    parser.add_argument("--index", required=True)
    parser.add_argument("--result-file", required=True)
    args = parser.parse_args()

    es = get_es_client(args.es_host)
    sns = boto3.client("sns")
    product_prefixes = [p.strip() for p in args.products.split(",")]

    results = []
    for prefix in product_prefixes:
        # Find the full product ID
        result = es.search(
            index=args.index,
            body={"query": {"match_phrase": {"id": prefix}}}
        )
        if result["hits"]["total"]["value"] == 0:
            results.append(f"ERROR: Product not found: {prefix}")
            continue

        product_id = result["hits"]["hits"][0]["_source"]["id"]

        # Verify CNM-S
        if wait_for_cnm_s(es, args.index, product_id):
            results.append(f"SUCCESS: CNM-S verified for {product_id}")
        else:
            results.append(f"ERROR: CNM-S timeout for {product_id}")
            continue

        # Mock CNM-R
        mock_cnm_r(sns, args.cnm_r_topic_arn, product_id)

        # Verify CNM-R
        if wait_for_cnm_r(es, args.index, product_id):
            results.append(f"SUCCESS: CNM-R verified for {product_id}")
        else:
            results.append(f"ERROR: CNM-R timeout for {product_id}")

    with open(args.result_file, "w") as f:
        f.write("\n".join(results) + "\n")

    # Exit non-zero if any errors
    if any("ERROR" in r for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()