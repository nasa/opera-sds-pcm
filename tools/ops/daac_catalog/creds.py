"""S3 clients for the DAAC buckets.

* `role`: the host's own AWS credentials (instance role). On SDS cluster hosts this lists and reads the ASF
  products bucket and its UMMG/ tree.
* `daac`: temporary keys from a DAAC `s3credentials` endpoint, obtained with Earthdata Login basic auth from
  `~/.netrc` (`machine urs.earthdata.nasa.gov`). The keys last one hour and only work in us-west-2. They are
  wrapped in botocore refreshable credentials, so a long run renews them before they expire.
* `auto`: `role` where the role is known to work (ASF products, UMMG/), `daac` everywhere else.

Credentials are never logged.
"""
from __future__ import annotations

import logging
import threading
from enum import StrEnum

import boto3
import requests
from botocore.config import Config
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session

from tools.ops.daac_catalog.registry import S3_REGION, Daac, DatasetSpec, Source
from tools.ops.daac_catalog.sources import normalize_ts

logger = logging.getLogger(__name__)

S3CREDENTIALS_ENDPOINTS = {
    Daac.ASF: "https://cumulus.asf.alaska.edu/s3credentials",
    Daac.PODAAC: "https://archive.podaac.earthdata.nasa.gov/s3credentials",
}


class CredsMode(StrEnum):
    AUTO = "auto"
    ROLE = "role"
    DAAC = "daac"


class CredentialsError(RuntimeError):
    pass


def fetch_s3credentials(endpoint: str, timeout: int = 60) -> dict:
    """Temporary S3 keys from a DAAC endpoint. Earthdata Login credentials come from ~/.netrc: requests applies
    netrc auth to the urs.earthdata.nasa.gov redirect and keeps the session cookies for the way back."""
    with requests.Session() as session:
        response = session.get(endpoint, timeout=timeout)
    if response.status_code != 200:
        raise CredentialsError(f"{endpoint} returned HTTP {response.status_code}")
    try:
        body = response.json()
    except ValueError:
        raise CredentialsError(f"{endpoint} did not return JSON (Earthdata Login netrc missing?)") from None
    for key in ("accessKeyId", "secretAccessKey", "sessionToken", "expiration"):
        if key not in body:
            raise CredentialsError(f"{endpoint} response lacks {key}")
    return body


def _botocore_metadata(body: dict) -> dict:
    return {"access_key": body["accessKeyId"], "secret_key": body["secretAccessKey"],
            "token": body["sessionToken"], "expiry_time": normalize_ts(body["expiration"]).isoformat()}


def client_config(max_pool_connections: int = 64) -> Config:
    return Config(region_name=S3_REGION, max_pool_connections=max_pool_connections,
                  retries={"max_attempts": 10, "mode": "adaptive"}, connect_timeout=20, read_timeout=120)


class S3Clients:
    """One thread-safe S3 client per (daac, mode), created on first use."""

    def __init__(self, mode: CredsMode | str = CredsMode.AUTO, max_pool_connections: int = 64,
                 profile: str | None = None, fetcher=fetch_s3credentials):
        self.mode = CredsMode(mode)
        self._pool = max_pool_connections
        self._profile = profile
        self._fetcher = fetcher
        self._clients: dict[tuple, object] = {}
        self._lock = threading.Lock()

    def uses_daac_credentials(self, spec: DatasetSpec, source: Source) -> bool:
        if self.mode == CredsMode.DAAC:
            return True
        if self.mode == CredsMode.ROLE:
            return False
        return not (spec.products_role_readable and source in (Source.PRODUCTS, Source.UMMG))

    def for_source(self, spec: DatasetSpec, source: Source):
        if self.uses_daac_credentials(spec, source):
            return self._get(("daac", spec.daac), lambda: self._daac_client(spec.daac))
        return self._get(("role",), self._role_client)

    def _get(self, key, factory):
        with self._lock:
            if key not in self._clients:
                self._clients[key] = factory()
            return self._clients[key]

    def _role_client(self):
        session = boto3.Session(profile_name=self._profile) if self._profile else boto3.Session()
        return session.client("s3", config=client_config(self._pool))

    def _daac_client(self, daac: Daac):
        endpoint = S3CREDENTIALS_ENDPOINTS[daac]

        def refresh():
            logger.info(f"Fetching temporary S3 credentials from {endpoint}")
            return _botocore_metadata(self._fetcher(endpoint))

        credentials = RefreshableCredentials.create_from_metadata(
            metadata=refresh(), refresh_using=refresh, method="daac-s3credentials")
        botocore_session = get_session()
        botocore_session._credentials = credentials
        botocore_session.set_config_variable("region", S3_REGION)
        return boto3.Session(botocore_session=botocore_session).client("s3", config=client_config(self._pool))
