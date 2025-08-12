"""Utility functions and classes used to interface with the ESA Dataspace service"""

from datetime import datetime, timedelta
from threading import Lock
from typing import Tuple

import backoff
import requests

from opera_commons.logger import logger
from util.backoff_util import fatal_code, backoff_logger

DEFAULT_DATASPACE_ENDPOINT = 'dataspace.copernicus.eu'
"""Default endpoint for pulling Dataspace credentials from netrc"""

DEFAULT_QUERY_ENDPOINT = 'https://catalogue.dataspace.copernicus.eu/odata/v1/Products'
"""Default URL endpoint for the Copernicus Data Space Ecosystem (CDSE) query REST service"""

DEFAULT_AUTH_ENDPOINT = 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'
"""Default URL endpoint for performing user authentication with CDSE"""

DEFAULT_DOWNLOAD_ENDPOINT = 'https://zipper.dataspace.copernicus.eu/odata/v1/Products'
"""Default URL endpoint for CDSE download REST service"""

DEFAULT_SESSION_ENDPOINT = 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/account/sessions'
"""Default URL endpoint to manage authentication sessions with CDSE"""


class NoQueryResultsException(Exception):
    """Custom exception to identify empty results from a query"""
    pass


class NoSuitableOrbitFileException(Exception):
    """Custom exception to identify no orbit files meeting overlap criteria"""


class DataspaceSession:
    """
    Context manager class to wrap credentialed operations in.
    Creates a session and gets its token on entry, and deletes the session on exit.
    """

    def __init__(self, username, password):
        self.__lock = Lock()
        self.__token, self.__session, self.__refresh_token, self.__expires = self._get_token(username, password)

    @property
    def token(self) -> str:
        """
        Get the current access token for the Dataspace session.
        If it is expired, it will attempt to refresh.
        """

        with self.__lock:
            if datetime.now() > self.__expires:
                self.__token, self.__session, self.__refresh_token, self.__expires = self._refresh()

            return self.__token

    @backoff.on_exception(backoff.constant,
                          requests.exceptions.RequestException,
                          max_time=300,
                          giveup=fatal_code,
                          on_backoff=backoff_logger,
                          interval=15)
    def _refresh(self) -> Tuple[str, str, str, datetime]:
        """
        Performs an access token refresh request using the refresh token acquired
        during initial authentication.

        Returns
        -------
        access_token : str
            The refreshed access token.
        session_id : str
            The ID associated with the authenticated session. Should be used to
            request deletion of the session once the desired orbit file(s) is downloaded.
        refresh_token : str
            Token used to refresh the access token prior to its next expiration.
        expiration_time : datetime.datetime
            The time when the access token is next expected to expire.

        """
        data = {
            'client_id': 'cdse-public',
            'refresh_token': self.__refresh_token,
            'grant_type': 'refresh_token',
        }

        response = requests.post(DEFAULT_AUTH_ENDPOINT, data=data)

        logger.info(f'Refresh Dataspace token request: {response.status_code}')

        response.raise_for_status()

        try:
            response_json = response.json()

            access_token = response_json['access_token']
            session_id = response_json['session_state']
            refresh_token = response_json['refresh_token']
            expires_in = response_json['expires_in']
        except KeyError as e:
            raise RuntimeError(
                f'Failed to parse expected field "{str(e)}" from authentication response.'
            )

        logger.info(f'Refreshed Dataspace token for session {self.__session}')

        return access_token, session_id, refresh_token, datetime.now() + timedelta(seconds=expires_in)

    @backoff.on_exception(backoff.constant,
                          requests.exceptions.RequestException,
                          max_tries=2,
                          giveup=fatal_code,
                          on_backoff=backoff_logger,
                          interval=15)
    def _get_token(self, username: str, password: str) -> Tuple[str, str, str, datetime]:
        """
        Acquires an access token from the CDSE authentication endpoint using the
        credentials provided by the user.

        Parameters
        ----------
        username : str
            Username of the account to authenticate with.
        password : str
            Password of the account to authenticate with.

        Returns
        -------
        access_token : str
            The access token parsed from a successful authentication request.
            This token must be included with download requests for them to be valid.
        session_id : str
            The ID associated with the authenticated session. Should be used to
            request deletion of the session once the desired orbit file(s) is downloaded.
        refresh_token : str
            Token used to refresh the access token prior to its expiration.
        expiration_time : datetime.datetime
            The time when the access token is expected to expire.

        Raises
        ------
        RuntimeError
            If the authentication request fails, or an invalid response is returned
            from the service.

        """
        data = {
            'client_id': 'cdse-public',
            'username': username,
            'password': password,
            'grant_type': 'password'
        }

        response = requests.post(DEFAULT_AUTH_ENDPOINT, data=data)

        logger.info(f'Get Dataspace token for {username}: {response.status_code}')

        response.raise_for_status()

        try:
            response_json = response.json()

            access_token = response_json['access_token']
            session_id = response_json['session_state']
            refresh_token = response_json['refresh_token']
            expires_in = response_json['expires_in']
        except KeyError as e:
            raise RuntimeError(
                f'Failed to parse expected field "{str(e)}" from authentication response.'
            )

        logger.info(f'Created Dataspace session {session_id}')

        return access_token, session_id, refresh_token, datetime.now() + timedelta(seconds=expires_in)

    @backoff.on_exception(backoff.constant,
                          requests.exceptions.RequestException,
                          max_time=300,
                          giveup=fatal_code,
                          on_backoff=backoff_logger,
                          interval=15)
    def _delete_token(self):
        """
        Submits a delete request on the provided endpoint URL for the provided
        session ID. This function should always be called after successful authentication
        to ensure we don't leave too many active session open (and hit the limit of
        the service provider).
        """
        logger.info("Requesting deletion of open authentication session")

        url = f'{DEFAULT_SESSION_ENDPOINT}/{self.__session}'
        headers = {'Authorization': f'Bearer {self.token}', 'Content-Type': 'application/json'}

        response = requests.delete(url=url, headers=headers)

        logger.info(f'Delete request {response.url}: {response.status_code}')

        response.raise_for_status()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._delete_token()
