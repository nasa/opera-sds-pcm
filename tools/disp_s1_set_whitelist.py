import argparse
import json
from copy import deepcopy
from typing import Optional, Iterable, Tuple

from opensearchpy import OpenSearch

from opera_commons.es_connection import get_mozart_es
from opera_commons.logger import get_logger


GRQ_RULES_INDEX = 'user_rules-grq'
DISP_TRIGGER_RULE = 'trigger-SCIFLO_L3_DISP_S1'

logger = get_logger()


def get_parser():
    parser = argparse.ArgumentParser()

    main_grp = parser.add_mutually_exclusive_group(required=True)

    main_grp.add_argument(
        '--whitelist-regions',
        nargs='+',
        dest='regions',
        metavar='REGION',
        help='List of regions to whitelist'
    )

    main_grp.add_argument(
        '--disable-whitelist',
        action='store_false',
        dest='whitelist_enabled',
        help='Disable whitelisting'
    )

    parser.add_argument(
        '--allow-unknown',
        action='store_true',
        help='Allow unknown regions in whitelist'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Dry-run trigger rule changes'
    )

    main_grp.add_argument(
        '--get-query',
        action='store_true',
        help='Show current trigger rule query'
    )

    return parser


def get_trigger_rule(es_conn: OpenSearch, rule_name: str) -> dict:
    """
    Get trigger rule from OpenSearch by name

    Args:
        es_conn: OpenSearch connection
        rule_name: Trigger rule name

    Returns:
        The trigger rule definition as a python dict
    """
    results = es_conn.search(
        body={
            "query": {
                "term": {"rule_name": rule_name}
            }
        },
        index=GRQ_RULES_INDEX,
    )

    hits = results['hits']['hits']

    if not hits:
        raise ValueError(f'No trigger rule found for {rule_name}')
    if len(hits) > 1:
        raise ValueError(f'More than one trigger rule found for {rule_name}')

    return hits[0]


def update_trigger_rule_definition(
        rule: dict,
        whitelist: Optional[Iterable[str]],
        allow_unknown: bool
) -> Tuple[bool, dict, dict]:
    """
    Update trigger rule definition

    Args:
        rule: Trigger rule definition from `get_trigger_rule`
        whitelist: List of whitelisted regions or None to disable whitelisting
        allow_unknown: Allow unknown regions in whitelist

    Return:
        value: tuple(if rule was changed: bool, updated rule definition: dict, original rule definition: dict)
    """

    query_string = json.loads(rule['_source']['query_string'])

    # Normalize JSON dumps of query string in original so that comparison won't break if formatting differs
    rule['_source']['query_string'] = json.dumps(query_string)

    original_rule = deepcopy(rule)

    region_clause_idx = None

    for i, must_term in enumerate(query_string['bool']['must']):
        if list(must_term.get('terms', {None: None}).keys())[0] == "metadata.region_id":
            region_clause_idx = i

    if whitelist is None:
        if region_clause_idx is not None:
            query_string['bool']['must'].pop(region_clause_idx)
    else:
        if region_clause_idx is not None:
            query_string['bool']['must'][region_clause_idx]['terms']["metadata.region_id"] = [str(r) for r in whitelist]
        else:
            query_string['bool']['must'].append({
                'terms': {"metadata.region_id": [str(r) for r in whitelist]}
            })
            region_clause_idx = -1

        if allow_unknown:
            query_string['bool']['must'][region_clause_idx]['terms']["metadata.region_id"].append('UNKNOWN')

    rule['_source']['query_string'] = json.dumps(query_string)

    return rule != original_rule, rule, original_rule


def put_trigger_rule(es_conn: OpenSearch, rule: dict):
    """
    Update trigger rule definition in OpenSearch

    Args:
        es_conn: OpenSearch connection
        rule: Trigger rule definition to set
    """
    es_conn.update(
        index=GRQ_RULES_INDEX,
        id=rule['_id'],
        body={
            'doc': rule['_source'],
            'doc_as_upsert': True
        },
        refresh=True
    )


def main(
        es_conn: OpenSearch,
        whitelist: Optional[Iterable[str]],
        allow_unknown: bool = False,
        dry_run: bool = False,
):
    """
    Apply whitelisting updates to KSC trigger rules

    Args:
        whitelist: List of region IDs to whitelist, must be at least 1. If None, whitelisting will be disabled
        allow_unknown: Allow unknown regions in whitelist
        dry_run: Dry-run trigger rule changes
    """
    rule_doc = get_trigger_rule(es_conn, DISP_TRIGGER_RULE)

    updated, new_doc, old_doc = update_trigger_rule_definition(
        rule_doc,
        whitelist,
        allow_unknown
    )

    if not updated:
        logger.info(f'Trigger rule {DISP_TRIGGER_RULE} not updated (already defined as requested)')
    else:
        if dry_run:
            logger.info(f'Dry run update for trigger rule {DISP_TRIGGER_RULE}')
            logger.info(f'OLD DEF:\n{json.dumps(old_doc, indent=2)}')
            logger.info(f'NEW DEF:\n{json.dumps(new_doc, indent=2)}')
        else:
            put_trigger_rule(es_conn, new_doc)


if __name__ == '__main__':
    es_conn = get_mozart_es(logger).es

    parser = get_parser()

    args = parser.parse_args()

    if args.get_query:
        rule_doc = get_trigger_rule(es_conn, DISP_TRIGGER_RULE)
        query_string = json.loads(rule_doc['_source']['query_string'])
        print(json.dumps(query_string, indent=2))
        exit(0)

    if args.whitelist_enabled:
        whitelist = args.regions
    else:
        whitelist = None

    main(
        es_conn,
        whitelist,
        allow_unknown=args.allow_unknown,
        dry_run=args.dry_run,
    )
