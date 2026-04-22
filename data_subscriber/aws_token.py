from datetime import datetime, timezone

import backoff
import dateutil.parser
import requests
from requests import HTTPError
from requests.auth import HTTPBasicAuth

from opera_commons.logger import get_logger
from util.backoff_util import backoff_logger


def supply_token(edl: str, username: str, password: str) -> str:
    """
    :param edl: Earthdata login (EDL) endpoint
    :param username: EDL username
    :param password:EDL password
    """
    logger = get_logger()
    token_list = _get_tokens(edl, username, password)
    _revoke_expired_tokens(token_list, edl, username, password)
    if not token_list:
        logger.info('Creating new EDL token')
        token = _create_token(edl, username, password)
    else:
        logger.info('Using existing EDL token')
        token = token_list[0]["access_token"]

    return token


def _get_tokens(edl: str, username: str, password: str) -> list[dict]:
    list_response = _requests_get_tokens(edl, username, password)
    list_response.raise_for_status()
    return list_response.json()


@backoff.on_exception(
    backoff.expo,
    exception=(HTTPError,),
    max_time=120,
    on_backoff=backoff_logger,
)
def _requests_get_tokens(edl: str, username: str, password: str):
    return requests.get(f"https://{edl}/api/users/tokens", auth=HTTPBasicAuth(username, password))


def _revoke_expired_tokens(token_list: list[dict], edl: str, username: str, password: str) -> None:
    for token_dict in token_list:
        now = datetime.now(timezone.utc).date()
        expiration_date = dateutil.parser.parse(token_dict["expiration_date"]).now(timezone.utc).date()

        if expiration_date <= now:
            _delete_token(edl, username, password, token_dict["access_token"])
            del token_dict


def _create_token(edl: str, username: str, password: str) -> str:
    create_response = _requests_post_tokens(edl, username, password)
    create_response.raise_for_status()

    response_content = create_response.json()
    if "error" in response_content.keys():
        raise Exception(response_content["error"])

    token = response_content["access_token"]
    return token


@backoff.on_exception(
    backoff.expo,
    exception=(HTTPError,),
    max_time=120,
    on_backoff=backoff_logger,
)
def _requests_post_tokens(edl: str, username: str, password: str):
    return requests.post(f"https://{edl}/api/users/token", auth=HTTPBasicAuth(username, password))


def _delete_token(edl: str, username: str, password: str, token: str) -> None:
    logger = get_logger()
    url = f"https://{edl}/api/users/revoke_token"
    try:
        resp = requests.post(url, auth=HTTPBasicAuth(username, password), params={"token": token})
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Error deleting the token: {e}")

    logger.info("CMR token successfully deleted")