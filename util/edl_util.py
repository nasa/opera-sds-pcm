"""Utility functions and classes used to interface with the Earthdata Login service"""

import requests

DEFAULT_EDL_ENDPOINT = "urs.earthdata.nasa.gov"
"""Default endpoint for authenticating with EarthData Login"""


class SessionWithHeaderRedirection(requests.Session):
    """
    Class with an override of the requests.Session.rebuild_auth to maintain
    headers when redirected by EarthData Login.

    This code was adapted from the examples available here:
    https://urs.earthdata.nasa.gov/documentation/for_users/data_access/python
    """

    def __init__(self, username, password, auth_host=DEFAULT_EDL_ENDPOINT):
        super().__init__()
        self.auth = (username, password)
        self.auth_host = auth_host

    # Overrides from the library to keep headers when redirected to or from
    # the NASA auth host.
    def rebuild_auth(self, prepared_request, response):
        headers = prepared_request.headers
        url = prepared_request.url

        if "Authorization" in headers:
            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)
            if (original_parsed.hostname != redirect_parsed.hostname) and \
                    redirect_parsed.hostname != self.auth_host and \
                    original_parsed.hostname != self.auth_host:
                del headers["Authorization"]
