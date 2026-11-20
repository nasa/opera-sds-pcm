from opensearchpy import OpenSearch
from opensearchpy.helpers import scan

from dist_s1.tools.validation.state_config_inspection.config import config

ENVIRONMENTS: dict[str, str] = {
    "local": "https://localhost:9200",
    "int-pop1": "https://opera-int-mozart-pop1.jpl.nasa.gov/grq_es/",
    "int-fwd": "https://opera-int-mozart-fwd.jpl.nasa.gov/grq_es/",
    "ops-fwd": "https://opera-ops-mozart-fwd.jpl.nasa.gov/grq_es/",
    "ops-pop1": "https://opera-ops-mozart-cool.jpl.nasa.gov/grq_es/",
}


def fetch_state_config_docs_historical(client: OpenSearch):
    return scan_all(client, "grq_1.0_dist_s1-state-config")


def fetch_state_config_docs_forward(client: OpenSearch):
    return scan_all(client, "grq_1.0_dist_s1-fwd-state-config")


def scan_all(client: OpenSearch, index: str) -> list[dict]:
    return list(_scan_all_gen(10_000, client, index))


def _scan_all_gen(batch_size: int, client: OpenSearch, index: str):
    hits = scan(
        client,
        index=index,
        query={"query": {"match_all": {}}},
        size=batch_size,
        scroll="5m",
        request_timeout=60,
    )
    docs = ({"_id": h["_id"], **h["_source"]} for h in hits)
    return docs


def get_client(env: str) -> OpenSearch:
    if env == "local":
        return get_local_client()
    auth = (config["GRQ_USER"], config["GRQ_PASSWORD"])
    return OpenSearch(
        hosts=[ENVIRONMENTS[env]],
        http_compress=True,
        http_auth=auth,
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        timeout=60,
    )


def get_local_client() -> OpenSearch:
    url: str = "https://localhost:9200"
    auth = (config["GRQ_USER"], config["GRQ_PASSWORD"])
    return OpenSearch(
        hosts=[url],
        http_auth=auth if auth else None,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        timeout=60,
    )
