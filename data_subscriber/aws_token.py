from datetime import datetime

import requests
from requests.auth import HTTPBasicAuth

from commons.logger import get_logger


def supply_token(edl: str, username: str, password: str) -> str:
    """
    :param edl: Earthdata login (EDL) endpoint
    :param username: EDL username
    :param password:EDL password
    """
    token_list = _get_tokens(edl, username, password)

    _revoke_expired_tokens(token_list, edl, username, password)

    if not token_list:
        token = _create_token(edl, username, password)
    else:
        token = token_list[0]["access_token"]

    return token


def _get_tokens(edl: str, username: str, password: str) -> list[dict]:
    token_list_url = f"https://{edl}/api/users/tokens"

    list_response = requests.get(token_list_url, auth=HTTPBasicAuth(username, password))
    list_response.raise_for_status()

    return list_response.json()


def _revoke_expired_tokens(token_list: list[dict], edl: str, username: str, password: str) -> None:
    for token_dict in token_list:
        now = datetime.utcnow().date()
        expiration_date = datetime.strptime(token_dict["expiration_date"], "%m/%d/%Y").date()

        if expiration_date <= now:
            _delete_token(edl, username, password, token_dict["access_token"])
            del token_dict


def _create_token(edl: str, username: str, password: str) -> str:
    token_create_url = f"https://{edl}/api/users/token"

    create_response = requests.post(token_create_url, auth=HTTPBasicAuth(username, password))
    create_response.raise_for_status()

    response_content = create_response.json()

    if "error" in response_content.keys():
        raise Exception(response_content["error"])

    token = response_content["access_token"]

    return token


def _delete_token(edl: str, username: str, password: str, token: str) -> None:
    logger = get_logger()

    url = f"https://{edl}/api/users/revoke_token"

    try:
        resp = requests.post(url, auth=HTTPBasicAuth(username, password),
                             params={"token": token})
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Error deleting the token: {e}")

    logger.info("CMR token successfully deleted")
